package driver

import (
	"bytes"
	"os/exec"
	"strings"

	"code.cloudfoundry.org/lager"
)

type ZfsDriver struct {
	logger lager.Logger
	zfsBin string
}

func NewZfsDriver(
	logger lager.Logger,
	zfsBin string,
) *ZfsDriver {
	return &ZfsDriver{
		logger: logger,
		zfsBin: zfsBin,
	}
}

func (driver *ZfsDriver) CreateVolume(path string) error {
	_, _, err := driver.run(driver.zfsBin, "create", path)
	if err != nil {
		return err
	}

	return nil
}

func (driver *ZfsDriver) DestroyVolume(path string) error {
	_, _, err := driver.run(driver.zfsBin, "destroy", "-r", path)
	if err != nil {
		return err
	}

	return nil
}

func (driver *ZfsDriver) CreateCopyOnWriteLayer(path string, parent string) error {
	components := []string{}
	components = append(components, parent)
	components = append(components, path)
	snapshot := strings.Join(components, "@")

	_, _, err := driver.run(driver.zfsBin, "snapshot", snapshot)
	if err != nil {
		return err
	}

	_, _, err = driver.run(driver.zfsBin, "clone", snapshot, path)
	if err != nil {
		return err
	}

	return nil
}

func (driver *ZfsDriver) run(command string, args ...string) (string, string, error) {
	cmd := exec.Command(command, args...)

	logger := driver.logger.Session("run-command", lager.Data{
		"command": command,
		"args":    args,
	})

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}

	cmd.Stdout = stdout
	cmd.Stderr = stderr

	err := cmd.Run()

	loggerData := lager.Data{
		"stdout": stdout.String(),
		"stderr": stderr.String(),
	}

	if err != nil {
		logger.Error("failed", err, loggerData)
		return "", "", err
	}

	logger.Debug("ran", loggerData)

	return stdout.String(), stderr.String(), nil
}
