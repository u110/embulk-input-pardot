
check: g-check

build: g-build

g-%:
	./gradlew $(*)
