package config

import (
	"os"
	"reflect"

	"github.com/spf13/viper"
)

func LoadConfig(path string, cfg interface{}) error {
	viper.AddConfigPath(path)
	viper.SetConfigName("app")
	viper.SetConfigType("env")
	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err != nil {
		return err
	}
	return viper.Unmarshal(cfg)
}

func IsContainer() bool {
	switch os.Getenv("ENVIRONMENT") {
	case "docker", "k8s":
		return true
	}
	return false
}

func LoadEnv(cfg interface{}) error {
	v := viper.New()
	for _, key := range configKeys(cfg) {
		v.Set(key, os.Getenv(key))
	}
	return v.Unmarshal(cfg)
}

func configKeys(cfg interface{}) []string {
	val := reflect.ValueOf(cfg)
	if val.Kind() == reflect.Ptr {
		val = val.Elem()
	}
	if val.Kind() != reflect.Struct {
		return nil
	}

	t := val.Type()
	keys := make([]string, 0, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("mapstructure")
		if tag == "" {
			tag = t.Field(i).Name
		}
		keys = append(keys, tag)
	}
	return keys
}

func Load(path string, cfg interface{}) error {
	if IsContainer() {
		return LoadEnv(cfg)
	}
	return LoadConfig(path, cfg)
}
