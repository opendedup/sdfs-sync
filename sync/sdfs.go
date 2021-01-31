package fs

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"io/ioutil"

	"path/filepath"

	storage "cloud.google.com/go/storage"
	"github.com/kelseyhightower/envconfig"
	"github.com/olekukonko/tablewriter"
	sdfs "github.com/opendedup/sdfs-client-go/api"
	spb "github.com/opendedup/sdfs-client-go/sdfs"
	log "github.com/sirupsen/logrus"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"gopkg.in/yaml.v2"
)

//Config is the configuration of the Connection
type Config struct {
	Server struct {
		ServerURL    string `yaml:"url" default:"sdfss://localhost:6442" envconfig:"URL"`
		Password     string `yaml:"password" default:"" envconfig:"PASSWORD"`
		DisableTrust bool   `yaml:"disable_trust" envconfig:"DISABLE_TRUST"`
	} `yaml:"server"`
	Listener struct {
		Download bool     `yaml:"download" envconfig:"DOWNLOAD"`
		Upload   bool     `yaml:"upload" envconfig:"UPLOAD"`
		Write    bool     `yaml:"write" envconfig:"WRITE"`
		Delete   bool     `yaml:"delete" envconfig:"READ"`
		Ignore   []string `yaml:"ignore" default:".sdfsclitemp/"`
	} `yaml:"listener"`
	GCS struct {
		Credentials string `yaml:"credentials" envconfig:"GOOGLE_APPLICATION_CREDENTIALS"`
		BucketName  string `yaml:"bucket" envconfig:"GCS_BUCKET"`
		ProjectID   string `yaml:"projectid" envconfig:"GCS_PROJECT_ID"`
		BasePath    string `yaml:"base_path" evnconfig:"GCS_BASE_PATH"`
		Enabled     bool   `yaml:"enabled" envconfig:"GCS_ENABLED"`
		Region      string `yaml:"region" default:"US" envconfig:"GCS_REGION"`
		TempDir     string `yaml:"tempdir" default:"/tmp" envconfig:"GCS_TEMP_DIR"`
		Retry       int    `yaml:"retry" default:"3" envconfig:"GCS_RETRY"`
		client      *storage.Client
		bucket      *storage.BucketHandle
	} `yaml:"gcs"`
	FOLDER struct {
		Path        string `yaml:"base_path" envconfig:"LOCAL_PATH"`
		Owner       int64  `yaml:"owner" default:"0" envconfig:"LOCAL_OWNER"`
		Group       int64  `yaml:"group" default:"0" envconfig:"LOCAL_GROUP"`
		Permissions int32  `yaml:"permissions" default:"0" envconfig:"LOCAL_PERMISSIONS"`
		Enabled     bool   `yaml:"enabled" envconfig:"LOCAL_ENABLED"`
	} `yaml:"folder"`
}

//Listener is the listener object for executing on changes
type Listener struct {
	con    *sdfs.SdfsConnection
	config *Config
	debug  bool
}

//Listen captures changes and runs stuff
func (n *Listener) Listen(ctx context.Context) error {
	c := make(chan *spb.FileMessageResponse)
	go n.con.FileNotification(ctx, c)
	if n.config.GCS.Enabled {

		client, err := storage.NewClient(ctx, option.WithCredentialsFile(n.config.GCS.Credentials))

		if err != nil {
			return err
		}
		n.config.GCS.client = client
		bucket := client.Bucket(n.config.GCS.BucketName)
		n.config.GCS.bucket = bucket
		n.CreateGCSBucket(ctx)
	}
	for {
		fInfo := <-c
		if fInfo == nil {
			log.Info("done")
			return nil
		}
		var files []string
		for _, v := range fInfo.Response {
			files = append(files, v.FilePath)
			if n.debug {
				table := tablewriter.NewWriter(os.Stdout)
				table.SetHeader([]string{"File Name", v.FileName})
				atime := time.Unix(0, v.Atime*int64(time.Millisecond))
				ctime := time.Unix(0, v.Ctime*int64(time.Millisecond))
				mtime := time.Unix(0, v.Mtime*int64(time.Millisecond))
				table.Append([]string{"File Name", v.FileName})
				if v.Type == 0 {

					table.Append([]string{"Size", strconv.FormatInt(v.Size, 10)})
					table.Append([]string{"File GUID", v.FileGuild})
					table.Append([]string{"Map GUID", v.MapGuid})
					table.Append([]string{"File Path", v.FilePath})
					table.Append([]string{"Access Time", atime.String()})
					table.Append([]string{"Create Time", ctime.String()})
					table.Append([]string{"Modifies Time", mtime.String()})
					table.Append([]string{"Execute", fmt.Sprintf("%t", v.Execute)})
					table.Append([]string{"Read", fmt.Sprintf("%t", v.Read)})
					table.Append([]string{"Read", fmt.Sprintf("%t", v.Write)})
					table.Append([]string{"Hidden", fmt.Sprintf("%t", v.Hidden)})
					table.Append([]string{"Hash Code", fmt.Sprintf("%d", v.Hashcode)})
					table.Append([]string{"ID", v.Id})
					table.Append([]string{"Importing", fmt.Sprintf("%t", v.Importing)})
					table.Append([]string{"Unix Permissions", fmt.Sprintf("%d", v.Permissions)})
					table.Append([]string{"Group ID", fmt.Sprintf("%d", v.GroupId)})
					table.Append([]string{"User ID", fmt.Sprintf("%d", v.UserId)})
					table.Append([]string{"File Open", fmt.Sprintf("%t", v.Open)})
					table.Append([]string{"Symlink", fmt.Sprintf("%t", v.Symlink)})
					table.Append([]string{"Symlink Path", fmt.Sprintf("%s", v.SymlinkPath)})
					table.Append([]string{"File Type", fmt.Sprintf("%s", v.Type)})
				} else {
					table.Append([]string{"Size", strconv.FormatInt(v.Size, 10)})
					table.Append([]string{"File Path", v.FilePath})
					table.Append([]string{"Access Time", atime.String()})
					table.Append([]string{"Create Time", ctime.String()})
					table.Append([]string{"Modifies Time", mtime.String()})
					table.Append([]string{"Execute", fmt.Sprintf("%t", v.Execute)})
					table.Append([]string{"Read", fmt.Sprintf("%t", v.Read)})
					table.Append([]string{"Read", fmt.Sprintf("%t", v.Write)})
					table.Append([]string{"Hidden", fmt.Sprintf("%t", v.Hidden)})
					table.Append([]string{"Hash Code", fmt.Sprintf("%d", v.Hashcode)})
					table.Append([]string{"Unix Permissions", fmt.Sprintf("%d", v.Permissions)})
					table.Append([]string{"Group ID", fmt.Sprintf("%d", v.GroupId)})
					table.Append([]string{"User ID", fmt.Sprintf("%d", v.UserId)})
					table.Append([]string{"Symlink", fmt.Sprintf("%t", v.Symlink)})
					table.Append([]string{"Symlink Path", fmt.Sprintf("%s", v.SymlinkPath)})
					table.Append([]string{"File Type", fmt.Sprintf("%s", v.Type)})
				}
				table.Append([]string{"Event Type", fmt.Sprintf("%s", fInfo.Action)})
				table.SetAlignment(tablewriter.ALIGN_LEFT)

				table.Render()
			}
			ignore := false
			for _, s := range n.config.Listener.Ignore {
				if strings.HasPrefix(v.FilePath, s) {
					ignore = true
					log.Debugf("ignoring %s because it starts with %s", v.FilePath, s)
					break
				}
			}
			if !ignore {
				if fInfo.Action == spb.Syncaction_DOWNLOAD && n.config.Listener.Download {
					if n.config.GCS.Enabled {
						n.UploadToGCS(ctx, v)
					}
					if n.config.FOLDER.Enabled {
						n.CopyLocally(ctx, v)
					}

				}
				if fInfo.Action == spb.Syncaction_UPLOAD && n.config.Listener.Upload {
					if n.config.GCS.Enabled {
						n.UploadToGCS(ctx, v)
					}
					if n.config.FOLDER.Enabled {
						n.CopyLocally(ctx, v)
					}

				}
				if fInfo.Action == spb.Syncaction_WRITE && n.config.Listener.Write {
					if n.config.GCS.Enabled {
						n.UploadToGCS(ctx, v)
					}
					if n.config.FOLDER.Enabled {
						n.CopyLocally(ctx, v)
					}

				}
				if fInfo.Action == spb.Syncaction_DELETE && n.config.Listener.Delete {

				}
			}
		}
	}
}

//CopyLocally creates a local copy of the file
func (n *Listener) CopyLocally(ctx context.Context, resp *spb.FileInfoResponse) error {
	file := filepath.Join(n.config.FOLDER.Path, resp.FilePath)
	parent := filepath.Dir(file)
	err := os.MkdirAll(parent, os.ModePerm)
	if err != nil {
		log.Fatalf("creating directory %s : %v", parent, err)
		return fmt.Errorf("creating directory error %s : %v", file, err)
	}
	log.Debugf("Writing %s : %s", resp.FilePath, file)

	n.con.Download(ctx, resp.FilePath, file)
	log.Debugf("File %v written.\n", resp.FilePath)
	permissions := resp.Permissions
	group := resp.GroupId
	user := resp.UserId
	if n.config.FOLDER.Permissions > 0 {
		permissions = n.config.FOLDER.Permissions
	} else {
		b := fmt.Sprintf("%04d", permissions)

		z, err := strconv.ParseUint(b, 8, 32)
		permissions = int32(z)
		if err != nil {
			fmt.Println(err)
		}
	}
	if permissions > 0 {

		log.Debugf("Setting Permssions : %d", int(permissions))
		err = os.Chmod(file, os.FileMode(int(permissions)))

		if err != nil {
			log.Error(err)
		}
	}
	if n.config.FOLDER.Group > 0 {
		group = n.config.FOLDER.Group
	}
	if n.config.FOLDER.Owner > 0 {
		user = n.config.FOLDER.Owner
	}

	// Change file ownership.
	if user > 0 && group > 0 {
		log.Debugf("Setting UID : %d, GID : %d", int(user), int(group))
		err = os.Chown(file, int(user), int(group))
	}

	if err != nil {
		log.Error(err)
	}
	return nil

}

//UploadToGCS uploads a file to GCS
func (n *Listener) UploadToGCS(ctx context.Context, resp *spb.FileInfoResponse) error {
	file, err := ioutil.TempFile(n.config.GCS.TempDir, "sdfs*")
	if err != nil {
		return err
	}
	f, err := os.Open(file.Name())
	if err != nil {
		return fmt.Errorf("Open error %s : %v", file.Name(), err)
	}
	defer f.Close()
	defer os.Remove(file.Name())

	n.con.Download(ctx, resp.FilePath, file.Name())
	wc := n.config.GCS.bucket.Object(resp.FilePath).NewWriter(ctx)
	if _, err = io.Copy(wc, f); err != nil {
		return fmt.Errorf("Upload Error: %v", err)
	}
	if err := wc.Close(); err != nil {
		return fmt.Errorf("Close Error: %v", err)
	}
	log.Printf("Blob %v uploaded.\n", resp.FilePath)
	return nil

}

//CreateGCSBucket creates a GCS bucket if it does not exist
func (n *Listener) CreateGCSBucket(ctx context.Context) error {
	// Setup context and client

	buckets := n.config.GCS.client.Buckets(ctx, n.config.GCS.ProjectID)
	for {
		if n.config.GCS.BucketName == "" {
			return fmt.Errorf("BucketName entered is empty")
		}
		attrs, err := buckets.Next()
		// Assume bucket not found if at Iterator end and create
		if err == iterator.Done {
			// Create bucket
			if err := n.config.GCS.bucket.Create(ctx, n.config.GCS.ProjectID, &storage.BucketAttrs{
				Location: n.config.GCS.Region,
			}); err != nil {
				return fmt.Errorf("Failed to create bucket: %v", err)
			}
			log.Infof("Bucket %v created.\n", n.config.GCS.BucketName)
			return nil
		}
		if err != nil {
			return fmt.Errorf("Issues setting up Bucket(%q) Objects(): %v Double check project id", attrs.Name, err)
		}
		if attrs.Name == n.config.GCS.BucketName {
			log.Infof("Bucket %v exists\n", n.config.GCS.BucketName)
			return nil
		}
	}
}

// ValidateConfigPath just makes sure, that the path provided is a file,
// that can be read
func ValidateConfigPath(path string) error {
	s, err := os.Stat(path)
	if err != nil {
		return err
	}
	if s.IsDir() {
		return fmt.Errorf("'%s' is a directory, not a normal file", path)
	}
	return nil
}

// NewConfig returns a new decoded Config struct
func NewConfig(configPath string) (*Config, error) {
	// Create config structure
	config := &Config{}

	// Open config file
	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	// Init environmental variables
	err = envconfig.Process("", config)

	if err != nil {
		return nil, err
	}
	// Init new YAML decode
	d := yaml.NewDecoder(file)
	// Start YAML decoding from file
	if err := d.Decode(&config); err != nil {
		return nil, err
	}
	config.Server.ServerURL = strings.ToLower(config.Server.ServerURL)
	if !strings.HasPrefix(config.Server.ServerURL, "sdfs") {
		return nil, fmt.Errorf("unsupported server type %s, only supports sdfs:// or sdfss://", config.Server.ServerURL)
	}
	return config, nil
}

// NewsdfsListener returns a listener for a sdfs file system changes whose
// config file is the given config file. This node implements all NodeXxxxer
// operations available.
func NewsdfsListener(config string, debug bool) (*Listener, error) {
	if debug {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
	var err error
	err = ValidateConfigPath(config)
	if err != nil {
		return nil, err
	}
	conf, err := NewConfig(config)
	if err != nil {
		return nil, err
	}

	sdfs.DisableTrust = conf.Server.DisableTrust
	sdfs.Password = conf.Server.Password
	con, err := sdfs.NewConnection(conf.Server.ServerURL)
	if err != nil {
		return nil, err
	}
	sdfsListener := &Listener{debug: debug, con: con, config: conf}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err = con.GetVolumeInfo(ctx)
	if err != nil {
		return nil, err
	}

	return sdfsListener, nil
}
