// eslint-disable-next-line local-rules/mixed-imports
import type { Configuration } from "webpack";

declare function webpackConfigFunc(
    env: unknown,
    argv: { mode: "development" | "production"; watch: boolean }
): Configuration;

export = webpackConfigFunc;
