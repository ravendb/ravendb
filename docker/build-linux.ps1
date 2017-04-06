param([switch]$Run, [switch]$Wait)

docker build ./ravendb-linux -t ravendb;

if($LASTEXITCODE -eq 0 -and $Run) {
    if($Wait) {
        docker run -it --rm -p 8080:8080 -v db:/databases ravendb;
    } else {
        docker run -d -p 8080:8080 -v db:/databases ravendb;
    }
}