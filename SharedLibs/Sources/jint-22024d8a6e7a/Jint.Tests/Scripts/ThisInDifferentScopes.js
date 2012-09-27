function displayQuote() {
    // the value of 'this' will change; depends on 
    // which object it is called through
    return this.memorableQuote;
}

var williamShakespeare = {
    'memorableQuote': 'It is a wise father that knows his own child.',
    'sayIt': displayQuote
};

var markTwain = {
    'memorableQuote': 'Golf is a good walk spoiled.',
    'sayIt': displayQuote
};

var oscarWilde = {
    'memorableQuote': 'True friends stab you in the front.'
    // we can call the function displayQuote
    // as a method of oscarWilde without assigning it 
    // as oscarWilde’s method. 
    //'sayIt' : displayQuote
};

assert('It is a wise father that knows his own child.', williamShakespeare.sayIt()); // true, true
assert('Golf is a good walk spoiled.', markTwain.sayIt()); // he didn’t know where to play golf

// watch this, each function has a method call()
// that allows the function to be called as a 
// method of the object passed to call() as an
// argument. 
// this line below is equivalent to assigning
// displayQuote to sayIt, and calling oscarWilde.sayIt().
assert('True friends stab you in the front.', displayQuote.call(oscarWilde)); // ouch!
