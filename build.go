package main

import (
	"fmt"
	"os"
	"os/exec"
)

func main() {
	if len(os.Args) < 2 {
		printUsage()
		return
	}

	command := os.Args[1]
	switch command {
	case "build":
		build()
	case "run":
		run()
	case "test":
		test()
	case "clean":
		clean()
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Println("Usage: go run build.go [command]")
	fmt.Println("Commands:")
	fmt.Println("  build  - Build the LimeDB binary")
	fmt.Println("  run    - Run the LimeDB server")
	fmt.Println("  test   - Run tests")
	fmt.Println("  clean  - Clean build artifacts")
}

func build() {
	fmt.Println("Building LimeDB...")
	cmd := exec.Command("go", "build", "-o", "bin/limedb", "./cmd/limedb")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		fmt.Printf("Build failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Build successful!")
}

func run() {
	fmt.Println("Running LimeDB...")
	cmd := exec.Command("go", "run", "./cmd/limedb")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
}

func test() {
	fmt.Println("Running tests...")
	cmd := exec.Command("go", "test", "./...")
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Run()
}

func clean() {
	fmt.Println("Cleaning...")
	os.RemoveAll("bin/")
	fmt.Println("Clean complete!")
}
