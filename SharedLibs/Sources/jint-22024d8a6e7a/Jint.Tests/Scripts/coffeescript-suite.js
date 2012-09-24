
assert("var number;\nnumber = 42;", CoffeeScript.compile('number = 42', {bare:true}));