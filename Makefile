
check: g-check

build: g-build

g-%:
	./gradlew $(*)

preview: g-gem
	embulk preview -l warn -I build/gemContents/lib secret.yml
