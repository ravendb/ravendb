interface ShellAccessor {
    collapseMenu: KnockoutObservable<boolean>;
}

// React modules can be loaded while the Durandal shell module is still being initialized.
// Keeping the shell reference here avoids importing viewmodels/shell directly and hitting an AMD circular dependency.
let currentShell: ShellAccessor;

export function registerShell(shell: ShellAccessor) {
    currentShell = shell;
}

export function getShell() {
    return currentShell;
}
