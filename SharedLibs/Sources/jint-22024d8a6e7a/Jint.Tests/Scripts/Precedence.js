function Person() {
    this.first = "first";
    this.last = "last";
}

// in the parser, the NEW expression is appended by MEMBER, check that when it's a unary expression
// it is not seen as new (Person.first)
assert("first", (new Person).first);

