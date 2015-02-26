class execJs {
    static createSimpleCallableCode(script: string, context: Object): Function {
        return new Function("with(this) { try { " + script + " } catch(err) { return 'Evaluation Error:' +  err; } }").bind(context);
    }

    static validateSyntax(script: string): string {
        try {
            eval("try { " + script + " } catch (e) {}");
        } catch (e) {
            if (e instanceof SyntaxError) {
                return "Invalid binding syntax";
            }
        }

        return "";
    }
} 

export = execJs
