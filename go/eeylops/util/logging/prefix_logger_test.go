package logging

import (
	"testing"
)

func TestPrefixLogger(t *testing.T) {
	// The output should be seen on the screen.
	logger := NewPrefixLogger("logPrefixStr-1")
	logger2 := NewPrefixLoggerWithParent("logPrefixStr-2", logger)
	logger.Infof("Hello World!")
	logger.Infof("Hello World: %d", 2)
	logger.Infof("Hello World: %s", "jkaghkjahdkjhadkjha")
	logger2.Infof("Hello World!")
	logger2.Infof("Hello World: %d", 2)
	logger2.Infof("Hello World: %s", "jkaghkjahdkjhadkjha")

	logger.Warningf("Hello World!")
	logger.Warningf("Hello World: %d", 2)
	logger.Warningf("Hello World: %s", "jkaghkjahdkjhadkjha")
	logger2.Warningf("Hello World!")
	logger2.Warningf("Hello World: %d", 2)
	logger2.Warningf("Hello World: %s", "jkaghkjahdkjhadkjha")

	logger.Errorf("Hello World!")
	logger.Errorf("Hello World: %d", 2)
	logger.Errorf("Hello World: %s", "jkaghkjahdkjhadkjha")
	logger2.Errorf("Hello World!")
	logger2.Errorf("Hello World: %d", 2)
	logger2.Errorf("Hello World: %s", "jkaghkjahdkjhadkjha")

	logger.VInfof(0, "Hello World!")
	logger.VInfof(0, "Hello World: %d", 2)
	logger.VInfof(0, "Hello World: %s", "jkaghkjahdkjhadkjha")
	logger2.VInfof(0, "Hello World!")
	logger2.VInfof(0, "Hello World: %d", 2)
	logger2.VInfof(0, "Hello World: %s", "jkaghkjahdkjhadkjha")

	logger.VInfof(2, "Hello World!")
	logger.VInfof(2, "Hello World: %d", 2)
	logger.VInfof(2, "Hello World: %s", "jkaghkjahdkjhadkjha")
	logger2.VInfof(2, "Hello World!")
	logger2.VInfof(2, "Hello World: %d", 2)
	logger2.VInfof(2, "Hello World: %s", "jkaghkjahdkjhadkjha")
}
