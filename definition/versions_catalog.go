package definition

import (
	_ "embed"
	"strconv"
	"strings"
	"sync"

	"gopkg.in/yaml.v3"
)

const (
	componentTypePostgreSQL = "postgresql"
	componentTypePGBouncer  = "pgbouncer"
)

type versionsCatalog struct {
	ComponentTypes map[string]componentType `yaml:"componentTypes"`
}

type componentType struct {
	Versions []componentVersion `yaml:"versions"`
}

type componentVersion struct {
	Version string `yaml:"version"`
	Image   string `yaml:"image"`
	Default bool   `yaml:"default"`
}

var (
	//go:embed versions.yaml
	versionsYAML []byte

	catalogOnce sync.Once
	catalog     versionsCatalog
	catalogErr  error
)

// DefaultPGBouncerImage returns the default pgbouncer image from versions.yaml.
func DefaultPGBouncerImage() (string, bool) {
	return defaultImageForComponent(componentTypePGBouncer)
}

// DefaultPostgreSQLImage returns the default postgresql image from versions.yaml.
func DefaultPostgreSQLImage() (string, bool) {
	return defaultImageForComponent(componentTypePostgreSQL)
}

// PostgreSQLImageForVersion returns image for an exact PostgreSQL version from versions.yaml.
func PostgreSQLImageForVersion(version string) (string, bool) {
	loaded, ok := loadCatalog()
	if !ok {
		return "", false
	}

	component, ok := loaded.ComponentTypes[componentTypePostgreSQL]
	if !ok {
		return "", false
	}

	for _, v := range component.Versions {
		if v.Version == version && v.Image != "" {
			return v.Image, true
		}
	}

	return "", false
}

// PostgreSQLDefaultImageForMajor returns latest known image for major version from versions.yaml.
func PostgreSQLDefaultImageForMajor(major int) (string, bool) {
	loaded, ok := loadCatalog()
	if !ok {
		return "", false
	}

	component, ok := loaded.ComponentTypes[componentTypePostgreSQL]
	if !ok {
		return "", false
	}

	bestVersion := ""
	bestImage := ""
	for _, v := range component.Versions {
		if v.Image == "" || v.Version == "" || majorFromVersion(v.Version) != major {
			continue
		}
		if bestVersion == "" || compareDottedVersions(v.Version, bestVersion) > 0 {
			bestVersion = v.Version
			bestImage = v.Image
		}
	}

	if bestImage == "" {
		return "", false
	}

	return bestImage, true
}

func defaultImageForComponent(componentName string) (string, bool) {
	loaded, ok := loadCatalog()
	if !ok {
		return "", false
	}

	component, ok := loaded.ComponentTypes[componentName]
	if !ok {
		return "", false
	}

	for _, v := range component.Versions {
		if v.Default && v.Image != "" {
			return v.Image, true
		}
	}

	return "", false
}

func loadCatalog() (versionsCatalog, bool) {
	catalogOnce.Do(func() {
		catalogErr = yaml.Unmarshal(versionsYAML, &catalog)
	})

	if catalogErr != nil {
		return versionsCatalog{}, false
	}

	return catalog, true
}

func majorFromVersion(version string) int {
	parts := strings.SplitN(version, ".", 2)
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return -1
	}
	return major
}

func compareDottedVersions(a, b string) int {
	aParts := strings.Split(a, ".")
	bParts := strings.Split(b, ".")
	maxLen := len(aParts)
	if len(bParts) > maxLen {
		maxLen = len(bParts)
	}

	for i := 0; i < maxLen; i++ {
		av := 0
		if i < len(aParts) {
			if parsed, err := strconv.Atoi(aParts[i]); err == nil {
				av = parsed
			}
		}
		bv := 0
		if i < len(bParts) {
			if parsed, err := strconv.Atoi(bParts[i]); err == nil {
				bv = parsed
			}
		}

		if av > bv {
			return 1
		}
		if av < bv {
			return -1
		}
	}

	return 0
}
