"use strict";

var gulp = require("gulp"),
    concat = require("gulp-concat"),
    cssnano = require("gulp-cssnano"),
    concatCss = require('gulp-concat-css'),
    htmlmin = require("gulp-htmlmin"),
    uglify = require("gulp-uglify"),
    processhtml = require('gulp-processhtml'),
    del = require("del");


var PATHS = {
    outputDir: "../artifacts",
    cssSource: ["Content/bootstrap.css",
      "Content/bootstrap-datetimepicker.css",
      "Content/bootstrap-select.css",
      "Content/bootstrap-multiselect.css",
      "Content/font-awesome.css",
      "Content/durandal.css",
      "Content/nprogress.css",
      "Content/jquery-ui-1.10.4.custom.min.css",
      "Content/jquery.dynatree/skin/ui.dynatree.css",
      "Content/jquery.dynatree/custom-skin/custom.css",
      "Content/nv.d3.css",
      "Content/app.css",
      "Content/awesome-bootstrap-checkbox.css",
      "Content/animate.min.css"],
    jsSource: [
       "Scripts/jquery-2.1.3.js",
      "Scripts/jquery.blockUI.js",
      "Scripts/nprogress.js",
      "Scripts/knockout-3.2.0.js",
      "Scripts/knockout.dirtyFlag.js",
      "Scripts/knockout.mapping-2.4.1.js",
      "Scripts/knockout-delegatedEvents.min.js",
      "Scripts/knockout-postbox.min.js",
      "Scripts/moment.min.js",
      "Scripts/bootstrap.min.js",
      "Scripts/bootstrap-datetimepicker.min.js",
      "Scripts/bootstrap-contextmenu-2.1.js",
      "Scripts/bootstrap-multiselect.js",
      "Scripts/bootstrap-select.min.js",
      "Scripts/jquery-ui-1.10.4.custom.min.js",
      "Scripts/jquery.dynatree.js",
      "Scripts/jwerty-0.3.js",
      "Scripts/jquery.fullscreen.js",
      "Scripts/spin.min.js",
      "App/models/dto.js",
      "Scripts/analytics.js"
    ]
}

gulp.task("build-Debug", [/* Do nothing */]);
gulp.task("build-Release", ["release"]);
gulp.task("build-Profiling", [/* Do nothing */]);

gulp.task('release', ['min', 'release-process-index', 'release-copy-favicon', 'release-copy-optimized-build', 'release-copy-images', 'release-copy-fonts', 'release-copy-ext-libs']);

gulp.task("min", ["min:ext-js", "min:app-js", "min:css"]);

gulp.task('release-copy-ext-libs', function () {
    return gulp.src(["Scripts/ace/**/*.*",
        "Scripts/forge/**/*.*",
        "Scripts/moment.js",
        "Scripts/d3/**/*.*",
        "Scripts/text.js",
        "Scripts/require.js",
        "Scripts/jszip/**/*.*"], { base: 'Scripts/' })
        .pipe(uglify())
        .pipe(gulp.dest(PATHS.outputDir + "/Html5/Scripts/"))
});

gulp.task('release-copy-favicon', function () {
    return gulp.src("favicon.ico")
        .pipe(gulp.dest(PATHS.outputDir + "/Html5/"));
});

gulp.task('release-copy-optimized-build', function () {
    return gulp.src(['optimized-build/**/*.*', '!optimized-build/App/main.js'])
       .pipe(gulp.dest(PATHS.outputDir + "/Html5/"));
});

gulp.task('release-copy-images', function () {
    return gulp.src(['Content/**/*.gif', 'Content/**/*.jpg', 'Content/**/*.png'])
       .pipe(gulp.dest(PATHS.outputDir + "/Html5/Content/"));
});

gulp.task('release-copy-fonts', function () {
    return gulp.src('fonts/*')
       .pipe(gulp.dest(PATHS.outputDir + "/Html5/fonts"));
});

gulp.task('release-process-index', function () {
    return gulp.src('index.html')
        .pipe(processhtml())
        .pipe(gulp.dest(PATHS.outputDir + "/Html5"));
});

gulp.task("min:app-js", function () {
    return gulp.src(["optimized-build/App/main.js"])
        .pipe(concat(PATHS.outputDir + "/Html5/App/main.js"))
        .pipe(uglify())
        .pipe(gulp.dest("."));
});

gulp.task("min:ext-js", function () {
    return gulp.src(PATHS.jsSource, { base: "." })
           .pipe(concat('../artifacts/Html5/App/libs.min.js'))
           .pipe(uglify())
           .pipe(gulp.dest("."));
});

gulp.task("min:css", function () {
    return gulp.src(PATHS.cssSource, { base: "Content" })
           .pipe(concatCss("styles.min.css", { rebaseUrls: true }))
           .pipe(cssnano())
           .pipe(gulp.dest("../artifacts/Html5/Content/"));
});

gulp.task("clean", function () {
    del.sync([PATHS.outputDir + "/Html5/", PATHS.outputDir + "/Raven.Studio.Html5.zip"], { force: true });
});
