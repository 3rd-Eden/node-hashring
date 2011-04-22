/**
 * Calculates the index of the Array where item X should be placed, assuming the Array is sorted.
 *
 * @param {Array} array The array containing the items.
 * @param {Number} x The item that needs to be added to the array.
 * @param {Number} low Inital Index that is used to start searching, optional.
 * @param {Number} high The maximum Index that is used to stop searching, optional.
 * @returns {Number} the index where item X should be placed
 */
exports.Bisection = function bisection(array, x, low, high){
  // The low and high bounds the inital slice of the array that needs to be searched
  // this is optional
  low || (low = 0);
  high || (high = array.length);
  
  var mid;
  
  if (low < 0) throw new Error('Low must be a non-negative integer');
  
  while(low < high){
    mid = (low + high) >> 1;
    
    if (x < array[mid]){
      high = mid;
    } else {
      low = mid + 1;
    }
  }
  
  return low;
};
