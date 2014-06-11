class execJs {
    static createSimpleCallableCode(script: string, context: Object): Function {
        return new Function("with(this) { try { " + script + " } catch(err) { console.log('Evaluation Error:', err); } }").bind(context);
    }
} 

export = execJs
