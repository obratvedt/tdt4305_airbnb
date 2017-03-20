package tasks;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.List;

/**
 * Created by sigurd on 3/16/17.
 *
 * This class is used as a wrapper around the JSONobject recieved when reading 'neighbourhoods.geojson'
 * It has utility methods that makes it easier to access that data within the specific json-structure that is received
 *
 */
class Area implements Serializable {

    JSONObject obj;

    /**
     * Takes in one of the elements that is in the JSONArray retrived by jsonObject.get('features')
     * from the object that is read from the 'neighbourhoods.geojson' file
     * @param obj
     */
    Area(JSONObject obj){
        this.obj = obj;
    }

    public JSONObject getType(){
        return (JSONObject) this.obj.get("type");
    }

    private JSONObject getProps(){
        return (JSONObject) this.obj.get("properties");
    }

    /**
     *
     * @return name of the neighbourhood
     */
    public String getNeighbourhood(){
        return (String) this.getProps().get("neighbourhood");
    }

    public String getNeighbourhoodGroup(){
        return (String) this.getProps().get("neighbourhood_group");
    }

    public JSONObject getGeometry(){
        return (JSONObject) this.obj.get("geometry");
    }

    /**
     *
     * @return A list with all the coordinates retreived from the innermost array in the coordinates as a JSONArray with
     * first the lon at index 0 and the lat at index 1
     */
    public List<JSONArray> getCoordinates(){
        JSONArray firstArray = (JSONArray) this.getGeometry().get("coordinates");
        JSONArray secondArray = (JSONArray) firstArray.get(0);
        JSONArray finalArray = (JSONArray) secondArray.get(0);
        return finalArray.subList(0,finalArray.size());
    }

    public String getGeoType(){
        return (String) this.getGeometry().get("type");
    }
}
