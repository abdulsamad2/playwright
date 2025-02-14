class Cluster {
    constructor(data) {
      this.data = data;
      this.children = [];
    }
  }
  function createClusterFromString(input) {
    const stack = [];
    let currentCluster = null;
    let currentData = '';
  
    for (let i = 0; i < input.length; i++) {
      const char = input[i];
  
      if (char === '[') {
        const newCluster = new Cluster(currentData);
        currentData = '';
  
        if (currentCluster) {
          stack.push(currentCluster);
          currentCluster.children.push(newCluster);
        }
        currentCluster = newCluster;
      } else if (char === ']') {
        if (currentData !== '') {
          currentCluster.children.push(new Cluster(currentData));
          currentData = '';
        }
  
        if (stack.length > 0) {
          currentCluster = stack.pop();
        }
      } else if (char === ',') {
        if (currentData !== '') {
          currentCluster.children.push(new Cluster(currentData));
          currentData = '';
        }
      } else {
        currentData += char;
      }
    }
  
    return currentCluster;
  }
  function mergeClusters(cluster, parentKey = '') {
    const result = {};
  
    for (const child of cluster.children) {
      const { data, children } = child;
      const concatenatedKey = parentKey + data;
  
      if (children.length > 0) {
        result[concatenatedKey] = mergeClusters(child, concatenatedKey);
      } else {
        result[concatenatedKey] = {};
      }
    }
  
    return result;
  }
  function convertObjectToArray(obj) {
    const result = [];
  
    for (const key in obj) {
      if (Object.prototype.hasOwnProperty.call(obj, key)) {
        const value = obj[key];
  
        if (typeof value === 'object' && Object.keys(value).length > 0) {
          const childArray = convertObjectToArray(value);
          result.push(childArray);
        } else {
          result.push(key);
        }
      }
    }
  
    return result;
  } 
  function hasNestedArray(arr) {
    for (let i = 0; i < arr.length; i++) {
      if (Array.isArray(arr[i])) {
        return true; // Found a nested array
      }
    }
    return false; // No nested array found
  } 
  function getSeatsBatch(input) {
    let returnArray = [];
    function createSeateBatch(itemArray) {
  
      if (hasNestedArray(itemArray)) {
        for (let j of itemArray) {
          createSeateBatch(j)
        }
      }
      else {
        if (typeof (itemArray) == "object")
          returnArray.push([...itemArray])
        else
          returnArray.push([itemArray])
      }
  
    }
    // step 1
    const cluster = createClusterFromString(input);
  
    //step 2
    const mergedCluster = cluster && cluster?.data && cluster?.data != null ? mergeClusters(cluster, cluster.data) : undefined;
    //step 3
    const arrayData = mergedCluster ? convertObjectToArray(mergedCluster) : undefined;
  
    if (arrayData) {
      createSeateBatch(arrayData);
      return returnArray;
    }
    else {
      return undefined;
    }
  } 
  
   const GenerateNanoPlaces = (data) => {
    let returnData = [];
    
    data.map(x => {
       if (x?.places && x?.places.length > 0) {
        let _uData = getSeatsBatch(x?.places[0]);
        if (_uData) {
          _uData.map(y => {
            returnData.push({
              row: "",
              section: "",
              selection: x?.inventoryTypes.length > 0 ? x?.inventoryTypes[0] : "",
              offerId: x?.offers.length > 0 ? x?.offers.length>1?x?.offers.reduce((shortest, current) => {
                return current.length < shortest.length ? current : shortest;
              }):x?.offers[0] : "",
              listingId: "",
              places: y,
              seats: [],
              lowSeat: 0,
              highSeat: 0,
              count: 0,
              accessibility:x.accessibility.length>0 && x.accessibility.includes("wheelchair")?"wheelchair":x.accessibility.length>0?x.accessibility[0]:"",
              descriptionId:x.description,
              attributes:x.attributes
            })
          })
        }
        else {
          returnData.push({
            row: "",
            section: "",
            selection: x?.inventoryTypes.length > 0 ? x?.inventoryTypes[0] : "",
            offerId: x?.offers.length > 0 ? x?.offers.length>1?x?.offers.reduce((shortest, current) => {
                return current.length < shortest.length ? current : shortest;
              }):x?.offers[0] : "",
            listingId: "",
            places: x?.places,
            seats: [],
            lowSeat: 0,
            highSeat: 0,
            count: 0,
            accessibility:x.accessibility.length>0 && x.accessibility.includes("wheelchair")?"wheelchair":x.accessibility.length>0?x.accessibility[0]:"",
            descriptionId:x.description,
            attributes:x.attributes
  
          })
  
        }
  
        
  
      }
      else
        return undefined;
  
    });
     return returnData;
  }

export default GenerateNanoPlaces