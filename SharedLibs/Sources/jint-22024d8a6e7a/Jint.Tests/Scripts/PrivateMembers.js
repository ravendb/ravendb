function Person(name, age) {
    this.getName = function() { return name; };
    this.setName = function(newName) { name = newName; };
    this.getAge = function() { return age; };
    this.setAge = function(newAge) { age = newAge; };
}

var ray = new Person('Ray', 31);
assert('Ray', ray.getName());
assert(31, ray.getAge());

ray.setName('Younger Ray');
ray.setAge(22);
assert('Younger Ray', ray.getName());
assert(22, ray.getAge());
