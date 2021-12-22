const system = require('durandal/system');

export function overrideSystem() {
    const acquire = system.acquire;
    system.acquire = function(moduleIdOrModule: any) {
        const isModule = typeof moduleIdOrModule !== 'string' && !(moduleIdOrModule instanceof Array);
        if (isModule) {
            return system.defer(function(dfd: any) {
                // If the moduleId is a function...
                if (moduleIdOrModule instanceof Function) {

                    if (moduleIdOrModule.prototype.getView) {
                        dfd.resolve(moduleIdOrModule);
                    } else {
                        // Execute the function, passing a callback that should be 
                        // called when the (possibly) async operation is finished
                        const result = moduleIdOrModule(function(err: any, module: any) {
                            if(err) { dfd.reject(err); }
                            dfd.resolve(module);
                        });

                        // Also allow shorthand `return` from the funcction, which 
                        // resolves the Promise with whatever was immediately returned
                        if (result !== undefined) {
                            dfd.resolve(result);
                        }
                    }
                }

                // If the moduleId is actually an object, simply resolve with it
                else {
                    dfd.resolve(moduleIdOrModule);
                }
            });
        }

        // super()
        return acquire.apply(this, arguments);
    };
}

