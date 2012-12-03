({
	baseUrl: "./",
	mainConfigFile: "./config.js",
	useSourceUrl: true,
	optimize: "uglify",
	paths: {
		requireLib: "../Scripts/require"
	},
	namespace: "RavenDBProfiler",
	name: "profiler",
	include: ["requireLib", "config"]
})