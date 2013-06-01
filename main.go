package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"code.google.com/p/rsc/fuse"
)

const DISABLE_WRITE = true

var (
	mountpoint = flag.String("mnt", "/mnt/cbfs", "mount point")
	root       = flag.String("root", "http://cbfs:8484/", "cbfs root url")
)

var rootURL *url.URL

func main() {
	flag.Parse()

	c, err := fuse.Mount("/mnt/cbfs")
	if err != nil {
		log.Fatal(err)
	}

	rootURL, err = url.Parse(*root)
	if err != nil {
		log.Fatal(err)
	}

	c.Serve(CBFS{})
}

type CBFS struct{}

func (CBFS) Root() (fuse.Node, fuse.Error) {
	return GetDir("/")
}

func GetDir(p string) (*Dir, fuse.Error) {
	u := *rootURL
	u.Path = path.Join("/.cbfs/list", p) + "/"
	u.RawQuery = "includeMeta=true"
	resp, err := http.Get(u.String())
	if err != nil {
		log.Printf("GetDir(%q) => %v", p, err)
		return nil, fuse.EIO
	}
	defer resp.Body.Close()

	var d Dir
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		log.Printf("GetDir(%q) => %v", p, err)
		if err != io.EOF {
			return nil, fuse.EIO
		}
		return &Dir{
			Path: path.Clean(p),
			at:   time.Now(),
		}, nil
	}
	for _, file := range d.Files {
		d.size += file.Size
	}
	for _, dir := range d.Dirs {
		d.size += dir.Size
	}
	d.at = time.Now()
	return &d, nil
}

type Dir struct {
	Path  string `json:"path"`
	Files map[string]struct {
		Size     uint64    `json:"length"`
		Modified time.Time `json:"modified"`
	} `json:"files"`
	Dirs map[string]struct {
		Size uint64 `json:"size"`
	} `json:"dirs"`

	// internal fields
	size  uint64
	stale bool
	at    time.Time
}

func (d *Dir) Attr() fuse.Attr {
	if d.stale || time.Since(d.at) > time.Minute {
		n, err := GetDir(d.Path)
		if err == nil {
			*d = *n
		}
	}
	return fuse.Attr{
		Mode: os.ModeDir | 0755,
		Size: d.size,
	}
}

func (d *Dir) Lookup(name string, intr fuse.Intr) (fuse.Node, fuse.Error) {
	d.stale = true
	if d.stale || time.Since(d.at) > time.Minute {
		n, err := GetDir(d.Path)
		if err != nil {
			return nil, err
		}
		*d = *n
	}

	if _, ok := d.Files[name]; ok {
		u := *rootURL
		u.Path = path.Join(d.Path, name)
		resp, err := http.Get(u.String())
		if err != nil {
			log.Printf("Lookup(%q) => %v", name, err)
			return nil, fuse.EIO
		}

		body, err := ioutil.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			log.Printf("Lookup(%q) => %v", name, err)
			return nil, fuse.EIO
		}

		if resp.StatusCode != http.StatusOK {
			log.Printf("Lookup(%q) => %v", name, resp.Status)
			return nil, fuse.ENOENT
		}

		t, err := time.Parse(http.TimeFormat, resp.Header.Get("Last-Modified"))
		if err != nil {
			log.Printf("Lookup(%q) => %v", name, err)
			// keep going
		}

		return &File{
			Path:     u.RequestURI(),
			Body:     body,
			Modified: t,

			at: time.Now(),
		}, nil
	}
	if _, ok := d.Dirs[name]; ok {
		return GetDir(path.Join(d.Path, name))
	}
	return nil, fuse.ENOENT
}

func (d *Dir) ReadDir(intr fuse.Intr) ([]fuse.Dirent, fuse.Error) {
	if d.stale || time.Since(d.at) > time.Minute {
		n, err := GetDir(d.Path)
		if err != nil {
			return nil, err
		}
		*d = *n
	}

	var ents []fuse.Dirent

	i := uint64(1)

	for dir := range d.Dirs {
		i++
		ents = append(ents, fuse.Dirent{
			Inode: i,
			Name:  dir,
		})
	}

	for file := range d.Files {
		i++
		ents = append(ents, fuse.Dirent{
			Inode: i,
			Name:  file,
		})
	}

	d.stale = true // re-read next time

	return ents, nil
}

func (d *Dir) Create(r *fuse.CreateRequest, w *fuse.CreateResponse, intr fuse.Intr) (fuse.Node, fuse.Handle, fuse.Error) {
	if DISABLE_WRITE {
		return nil, nil, fuse.EPERM
	}

	u := *rootURL
	u.Path = path.Join(d.Path, r.Name)
	req, err := http.NewRequest("PUT", u.String(), nil)
	if err != nil {
		log.Printf("Create(%q) => %v", u.Path, err)
		return nil, nil, fuse.EIO
	}
	if int(r.Flags)&os.O_EXCL == os.O_EXCL {
		req.Header.Set("If-None-Match", "*")
	} else if int(r.Flags)&os.O_TRUNC != os.O_TRUNC {
		node, err := d.Lookup(r.Name, intr)
		if err == nil {
			return node, node.(fuse.Handle), nil
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Create(%q) => %v", u.Path, err)
		return nil, nil, fuse.EIO
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()

	d.stale = true

	f := &File{
		Path: u.Path,

		at: time.Now(),
	}
	return f, f, nil
}

func (d *Dir) Remove(r *fuse.RemoveRequest, intr fuse.Intr) fuse.Error {
	if DISABLE_WRITE {
		return fuse.EPERM
	}

	u := *rootURL
	u.Path = path.Join(d.Path, r.Name)
	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		log.Printf("Remove(%q) => %v", u.Path, err)
		return fuse.EIO
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("Remove(%q) => %v", u.Path, err)
		return fuse.EIO
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		log.Printf("Remove(%q) => %v", u.Path, resp.Status)
		return fuse.EIO
	}
	d.stale = true
	return nil
}

func (d *Dir) Rename(req *fuse.RenameRequest, newDir fuse.Node, intr fuse.Intr) fuse.Error {
	if DISABLE_WRITE {
		return fuse.EPERM
	}

	if d.stale || time.Since(d.at) > time.Minute {
		n, err := GetDir(d.Path)
		if err != nil {
			return err
		}
		*d = *n
	}

	oldFile, err := d.Lookup(req.OldName, intr)
	if err != nil {
		return err
	}

	switch f := oldFile.(type) {
	case *File:
		newFile, _, err := newDir.(*Dir).Create(&fuse.CreateRequest{
			Name: req.NewName,
		}, &fuse.CreateResponse{}, intr)
		if err != nil {
			return err
		}
		body, err := f.ReadAll(intr)
		if err != nil {
			return err
		}
		err = newFile.(*File).WriteAll(body, intr)
		if err != nil {
			return err
		}
		return d.Remove(&fuse.RemoveRequest{
			Name: req.OldName,
		}, intr)

	case *Dir:

	default:
		return fuse.EIO
	}

	return fuse.EIO
}

type File struct {
	Path     string
	Body     []byte
	Modified time.Time

	// internal fields
	stale bool
	at    time.Time
}

func (f *File) Attr() fuse.Attr {
	return fuse.Attr{
		Mode:  0644,
		Size:  uint64(len(f.Body)),
		Mtime: f.Modified,
	}
}

func (f *File) checkStale(intr fuse.Intr) bool {
	if f.stale || time.Since(f.at) > time.Minute {
		d, err := GetDir(path.Dir(f.Path))
		if err != nil {
			return true
		}
		node, err := d.Lookup(path.Base(f.Path), intr)
		if err != nil {
			return true
		}
		if file, _ := node.(*File); file == nil {
			return true
		} else {
			*f = *file
		}
	}
	return false
}

func (f *File) ReadAll(intr fuse.Intr) ([]byte, fuse.Error) {
	if f.checkStale(intr) {
		return nil, fuse.EIO
	}
	return f.Body, nil
}

func (f *File) WriteAll(b []byte, intr fuse.Intr) fuse.Error {
	if DISABLE_WRITE {
		return fuse.EPERM
	}
	if f.checkStale(intr) {
		return fuse.EIO
	}
	u := *rootURL
	u.Path = f.Path
	req, err := http.NewRequest("PUT", u.String(), bytes.NewReader(b))
	if err != nil {
		log.Printf("WriteAll(%q) => %v", f.Path, err)
		return fuse.EIO
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("WriteAll(%q) => %v", f.Path, err)
		return fuse.EIO
	}
	io.Copy(ioutil.Discard, resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		log.Printf("WriteAll(%q) => %v", f.Path, resp.Status)
		return fuse.EIO
	}

	f.stale = true

	return nil
}

func (f *File) Setattr(r *fuse.SetattrRequest, w *fuse.SetattrResponse, intr fuse.Intr) fuse.Error {
	if DISABLE_WRITE {
		return fuse.EPERM
	}
	f.stale = true // force read
	if f.checkStale(intr) {
		return fuse.EIO
	}

	if r.Valid&fuse.SetattrSize == fuse.SetattrSize {
		if r.Size < uint64(len(f.Body)) {
			return f.WriteAll(f.Body[:r.Size], intr)
		} else {
			return f.WriteAll(append(f.Body, make([]byte, r.Size-uint64(len(f.Body)))...), intr)
		}
	}

	return nil
}

func (f *File) Flush(r *fuse.FlushRequest, intr fuse.Intr) fuse.Error {
	if DISABLE_WRITE {
		return fuse.EPERM
	}
	return nil
}

func (f *File) Write(r *fuse.WriteRequest, w *fuse.WriteResponse, intr fuse.Intr) fuse.Error {
	if DISABLE_WRITE {
		return fuse.EPERM
	}
	f.stale = true // force read
	if f.checkStale(intr) {
		return fuse.EIO
	}

	log.Print(r.Offset)

	w.Size = len(r.Data)
	log.Print(len(f.Body[r.Offset:]), len(r.Data))
	if int64(len(f.Body)) < r.Offset+1 {
		f.Body = append(f.Body, make([]byte, int64(len(f.Body))-r.Offset+1)...)
	}
	if len(f.Body[r.Offset:]) > len(r.Data) {
		copy(f.Body[r.Offset:], r.Data)
	} else {
		f.Body = append(f.Body[:r.Offset], r.Data...)
	}
	return f.WriteAll(f.Body, intr)
}

func (f *File) Fsync(r *fuse.FsyncRequest, intr fuse.Intr) fuse.Error {
	f.stale = true
	if f.checkStale(intr) {
		return fuse.EIO
	}
	return nil
}
