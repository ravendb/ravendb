interface String {
  codePointAt();
  repeat();
  startsWith(str: string): boolean;
  endsWith(str: string): boolean;
  contains(str: string): boolean;
}

interface Array<T> {
  find(predicate: (element: T, index: number, array: Array<T>) => boolean, thisArg?: any): T;
  findIndex(predicate: (element: T, index: number, array: Array<T>) => boolean, thisArg?: any): T;
  keys(): ArrayIterator;
  entries(): ArrayIterator;
  values(): ArrayIterator;
}

interface  ArrayIterator {
  
}
