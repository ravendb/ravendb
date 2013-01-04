function DogConstructor(name) {
    this.name = name;
}

var spot = new DogConstructor('Spot');
assert('Spot', spot.name);

function DogConstructor2(name) {
    this.name = name;
    this.respondTo = function(name) {
        if(this.name == name) {
            return 'Woof';        
        }
    };
}

var spot = new DogConstructor2('Spot');
assert(undefined, spot.respondTo('Rover'));
assert('Woof', spot.respondTo('Spot')); // yeah!

assert(DogConstructor2, spot.constructor);