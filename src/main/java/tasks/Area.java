package tasks;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.Serializable;
import java.util.List;

/**
 * Created by sigurd on 3/16/17.
 */
class Area implements Serializable {

    JSONObject obj;

    Area(JSONObject obj){
        this.obj = obj;
    }

    public JSONObject getType(){
        return (JSONObject) this.obj.get("type");
    }

    private JSONObject getProps(){
        return (JSONObject) this.obj.get("properties");
    }

    public String getNeighbourhood(){
        return (String) this.getProps().get("neighbourhood");
    }

    public String getNeighbourhoodGroup(){
        return (String) this.getProps().get("neighbourhood_group");
    }

    public JSONObject getGeometry(){
        return (JSONObject) this.obj.get("geometry");
    }

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
