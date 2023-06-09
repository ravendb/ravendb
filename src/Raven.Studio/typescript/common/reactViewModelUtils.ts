export function getDirtyFlagForReact(dirtyFlag: () => DirtyFlag): KoToReactDirtyFlag {
    return {
        koIsDirty: dirtyFlag().isDirty,
        koSetIsDirty: (value: boolean) => value
            ? dirtyFlag().forceDirty()
            : dirtyFlag().reset(),
    };
}
