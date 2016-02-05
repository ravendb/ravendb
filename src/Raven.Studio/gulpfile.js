/// <binding AfterBuild='ts-dev-compile' />
var gulp = require('gulp'),
    ts = require('gulp-typescript'),
    sourcemaps = require("gulp-sourcemaps");

var paths = {
    tsSource: './typescript/**/*.ts',
    typings: './typings/**/*.d.ts',
    tsOutput: './wwwroot/App/'
};

var tsCompilerConfig = ts.createProject('tsconfig.json');

gulp.task('ts-dev-compile', function() {
    return gulp.src([paths.tsSource, paths.typings])
        .pipe(sourcemaps.init())
        .pipe(ts(tsCompilerConfig))
        .js
        .pipe(sourcemaps.write("."))
        .pipe(gulp.dest(paths.tsOutput));
});

gulp.task('watch', ['ts-dev-compile'], function () {
    gulp.watch(paths.tsSource, ['ts-dev-compile']);
});