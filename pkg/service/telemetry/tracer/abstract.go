package tracer

type ForkManager interface {
	ActiveDigest() string
	CheckForkSupported(forkDigest string) bool
	SetObservedDigest(forkDigest string)
}
