function Person(name) {
    this.name = name;
}

// standard instanciation
var james = new Person('James');
assert('James', james.name);

// using call() constructore
var james = { sn: 'Taylor' };
Person.call(james, 'James');
assert('James', james.name);
assert('Taylor', james.sn);