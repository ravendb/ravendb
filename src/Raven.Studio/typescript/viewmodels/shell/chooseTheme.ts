import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class chooseTheme extends dialogViewModelBase {
    
    static readonly themeLocalStorageKey = "raven-theme";
    static readonly stylesheetPrefix = "Content/css/";
    
    static readonly themeToStylesheet = {
        "dark" : "styles.css",
        "light": "styles-light.css",
        "blue": "styles-blue.css"
    } as dictionary<string>;
    
    useTheme(theme: string) {
        localStorage.setItem(chooseTheme.themeLocalStorageKey, theme);
        this.updateStylesheet(chooseTheme.themeToStylesheet[theme]);
        this.close();
    }
    
    private updateStylesheet(fileName: string) {
        const themeLink = document.getElementById("raven-theme") as HTMLLinkElement;
        themeLink.href = chooseTheme.stylesheetPrefix + fileName;
    }
}

export = chooseTheme;
