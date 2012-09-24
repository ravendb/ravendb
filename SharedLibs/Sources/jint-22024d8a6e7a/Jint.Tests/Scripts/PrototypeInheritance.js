function GreatDane() { }

var rover = new GreatDane();
var spot = new GreatDane();

GreatDane.prototype.getBreed = function() {
    return 'Great Dane';
};

// Works, even though at this point
// rover and spot are already created.
assert('Great Dane', rover.getBreed());

// this hides getBreed() in GreatDane.prototype
spot.getBreed = function() {
    return 'Little Great Dane';
};
assert('Little Great Dane', spot.getBreed());

// but of course, the change to getBreed 
// doesn’t propagate back to GreatDane.prototype
// and other objects inheriting from it,
// it only happens in the spot object
assert('Great Dane', rover.getBreed());


function A() {
    this.M1 = function() { return 'passed' };
}

A.prototype.M2 = function() { return this.M1(arguments); };

function B() { }
B.prototype = new A();

assert('passed', new A().M1());
assert('passed', new B().M1());
assert('passed', new B().M2());
assert(A, new B().constructor);

function C(arg1) { this.shouldUseOriginal = arg1; }
C.prototype = new B;
C.prototype.M2 = function() {
    if (this.shouldUseOriginal)
        return A.prototype.M2.apply(this, arguments);
    else
        return 'passed2';
}

assert('passed', new C(true).M2());
assert('passed2', new C(false).M2());

function Foo() {
}

Foo.prototype.name = 'Foo';

var baz = new Foo();
assert('Foo',baz.name);
baz.name = 'Baz';
assert('Foo',Foo.prototype.name);
assert('Baz',baz.name);
