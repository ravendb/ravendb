import inputCursor = require('common/inputCursor');

class autoCompleterSupport {
  constructor(private autoCompleteBase: KnockoutObservableArray<KnockoutObservable<string>>, private autoCompleteResults: KnockoutObservableArray<KnockoutObservable<string>>,private showAllOptionOnEmptyInput: boolean = false) {
  }

  searchForCompletions(input: JQuery) {
    this.autoCompleteResults([]);

    var typedWord = this.getWordUserIsTyping(input);

      if (typedWord.length >= 1 || this.showAllOptionOnEmptyInput) {
      this.autoCompleteResults(this.autoCompleteBase().filter((value) =>
        autoCompleterSupport.wordMatches(typedWord, value()) &&
        (value() !== input.val()) &&
        (value() !== typedWord) &&
        (value().indexOf(' ') === -1)
        ));
    }
  }

  completeTheWord(input: JQuery, selectedCompletion: string, updateObservableClouse: (newValue: string) => void = null) {
    if (input.length > 0) {
      var inputValue: string = input.val();
      var typedWord = this.getWordUserIsTyping(input);

      var cursorPosition = inputCursor.getPosition(input);
      var beginIndex = this.findWordStartWithEndPosition(inputValue, cursorPosition - 1) + 1;

      // update observable here as input has on key up update
      var newValue = inputValue.substring(0, beginIndex) +
            selectedCompletion +
            inputValue.substring(cursorPosition);

      input.val(newValue);
      if (updateObservableClouse) {
          updateObservableClouse(newValue);
      }

      var positionCorrection = 0;
      if (selectedCompletion[selectedCompletion.length-1] === ")" ) {
        positionCorrection = -1;
      }
      inputCursor.setPosition(input, beginIndex + selectedCompletion.length + positionCorrection);
      this.autoCompleteResults([]);
    }
  }

  private findWordStartWithEndPosition(inputValue: string, endPosition: number): number {
    var beginIndex = 0;
    for (beginIndex = endPosition; beginIndex >= 0; beginIndex--) {
      var charCode = inputValue.charCodeAt(beginIndex);
      // going back skip every alphanumeric characters
      if ((48 <= charCode && charCode <= 57) // char in range from '0' to '9'
        || (65 <= charCode && charCode <= 90) // char in range from 'A' to 'Z'
        || (97 <= charCode && charCode <= 122) // char in range from 'a' to 'z'
        || (charCode == 95) // char is '_'
        ) {
        continue;
      } else {
        break;
      }
    }
    return beginIndex;

  }
  private getWordUserIsTyping($input: JQuery) {
    var cursorPosition = inputCursor.getPosition($input);
    //var beginIndex = $input.val().lastIndexOf(' ', cursorPosition-1);
    var beginIndex = this.findWordStartWithEndPosition($input.val(), cursorPosition - 1) + 1;

    var endIndex = $input.val().indexOf(' ', cursorPosition);
    if (endIndex === -1) {
      endIndex = $input.val().length;
    }
    return $input.val().substring(beginIndex, cursorPosition).trim();
  }

  public static wordMatches(toCheck: string, toMatch: string): boolean {
    // ignore the case
    toCheck = toCheck.toLowerCase();
    toMatch = toMatch.toLowerCase();

    // match as long as the letters are in correct order
    var matchedChars = 0;
    for (var i = 0; i < toMatch.length; i++) {
      if (toCheck[matchedChars] === toMatch[i]) {
        matchedChars++;
      }
      if (matchedChars >= toCheck.length) {
        return true;
      }
    }
    return false;
  }
}

export = autoCompleterSupport;