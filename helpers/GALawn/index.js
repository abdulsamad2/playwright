const LAWNGASeats=16;
export const AttachGALawnRow =(data,offers,event)=>{
    let finalLineDataWithoutPrice=[];
    if(data.length>0)
    {
        //attach offer with offer id
      const updatedOffers=data.map(x=>{
        return {
            ...x,
            offers:x.offers?.map(z=>{
                let offerGet=offers.find(y=>y?.offerId==z);
                if(offerGet!=null)
                {
                    return offerGet
                }
                else
                return undefined;
            })
            
            
    
        }
      })
        //check if it has more than one offer type
        updatedOffers.filter(x=>x!=undefined)
        //.filter(x=>x.offers.length<=1)
        //create seats and row
        .forEach((item) => {
            const { count, section:sec } = item;
            const rowCount = Math.ceil(count / LAWNGASeats);
        
            for (let row = 1; row <= rowCount; row++) {
              const remainingSeats = Math.min(count - (row - 1) * LAWNGASeats, LAWNGASeats);
              const seats = Array.from({ length: remainingSeats }, (_, index) => index + 1);
              finalLineDataWithoutPrice.push({
                row: row.toString(),
                section:sec,
                amount:undefined,
                lineItemType:undefined,
                seats:seats,
                offers:item.offers,
                offerId:item.offerId,
                accessibility:item.accessibility,
                descriptionId:item.descriptionId,
                attributes:item.attributes
                
                
                
                
              });
            }
          })  
    }

    return finalLineDataWithoutPrice;
    

}

const CreateGAandLawnLines=()=>{

}