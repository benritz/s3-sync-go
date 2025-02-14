package syncing

import (
	"benritz/s3sync/internal/paths"
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
)

type SyncPrinter struct {
	Result   chan *SyncResult
	Progress chan *SyncProgress
	wg       sync.WaitGroup
}

func NewSyncPrinter(ctx context.Context, srcRoot *paths.Path) *SyncPrinter {
	result := make(chan *SyncResult)
	progress := make(chan *SyncProgress)

	p := &SyncPrinter{
		Result:   result,
		Progress: progress,
	}

	p.start(ctx, srcRoot)

	return p
}

func (p *SyncPrinter) Close() {
	close(p.Result)
	close(p.Progress)
	p.wg.Wait()
}

func isTerminal(ctx context.Context) bool {
	fi, err := os.Stdout.Stat()

	if err != nil {
		slog.ErrorContext(ctx, "failed to get stdout stat", "err", err)
		return false
	}

	return (fi.Mode() & os.ModeCharDevice) != 0
}

func (p *SyncPrinter) start(ctx context.Context, srcRoot *paths.Path) {
	p.wg.Add(1)

	go func() {
		defer p.wg.Done()

		// print with option to delete the text on the next print
		// used to update the progress
		// only print deleteable text if the stdout is a terminal
		isTerminal := isTerminal(ctx)
		cursorSaved := false

		print := func(line string, deleteable bool) {
			if isTerminal {
				if cursorSaved {
					fmt.Print("\033[u")  // restore cursor position
					fmt.Print("\033[0K") // clear entire screen after cursor
				}
				if deleteable {
					fmt.Print("\033[s") // save cursor position
					cursorSaved = true
				} else {
					cursorSaved = false
				}
			} else if deleteable {
				return
			}

			fmt.Print(line)
		}

		// store the progress for a path to display the progress of the uploads
		// the printed progress is deletable to only show the latest progress
		progress := make(map[string]*SyncProgress)

		printProgress := func() {
			if len(progress) == 0 {
				return
			}

			items := make([]string, 0, len(progress))
			for _, ret := range progress {
				items = append(items, fmt.Sprintf("%s %d%%", ret.SrcPath.Path, ret.Progress))
			}

			print(fmt.Sprintf("uploading: %s\n", strings.Join(items, ", ")), true)
		}

		for {
			select {
			case <-ctx.Done():
				return
			case ret, ok := <-p.Progress:
				if !ok {
					return
				}

				progress[ret.SrcPath.Path] = ret
				printProgress()
			case ret, ok := <-p.Result:
				if !ok {
					return
				}

				srcPath := ret.SrcPath.Path
				rel := srcRoot.GetRel(srcPath)

				var line string

				switch ret.Outcome {
				case Error:
					line = fmt.Sprintf("failed: %s %v\n", rel, ret.Err)
				case Skip:
					line = fmt.Sprintf("skipped: %s\n", rel)
				case MetadataOnly:
					line = fmt.Sprintf("updated metadata: %s %v\n", rel, ret.MissingAlgorithms)
				case Copied:
					line = fmt.Sprintf("copied: %s\n", rel)
				}

				print(line, false)

				delete(progress, srcPath)
				printProgress()
			}
		}
	}()
}
