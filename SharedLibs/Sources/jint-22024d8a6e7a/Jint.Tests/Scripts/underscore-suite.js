
var lyrics = [
  {line : 1, words : "I'm a lumberjack and I'm okay"},
  {line : 2, words : "I sleep all night and I work all day"},
  {line : 3, words : "He's a lumberjack and he's okay"},
  {line : 4, words : "He sleeps all night and he works all day"}
];

var result = _(lyrics).chain()
  .map(function(line) { return line.words.split(' '); })
  .flatten()
  .reduce(function(counts, word) {
    counts[word] = (counts[word] || 0) + 1;
    return counts;
}, {}).value();

assert(2, result['lumberjack']);
assert(4, result['all']);
assert(2, result['night']);