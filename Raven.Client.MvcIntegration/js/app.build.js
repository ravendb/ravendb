({
    baseUrl: "./",
    mainConfigFile: "./config.js",
    useSourceUrl: true,
    optimize: "uglify",
    paths: {
        requireLib: "require"
    },
    namespace: "ravenprofiler",
    name: "profiler",
    include: ["requireLib", "config"]
})