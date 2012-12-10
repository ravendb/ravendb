({
	baseUrl: "./",
	mainConfigFile: "config.js",
	useSourceUrl: true,
	optimize: "uglify",
	paths: {
		requireLib: "vendor/require"
	},
	namespace: "RavenDBProfiler",
	name: "profiler",
	include: ["requireLib", "config"]
})