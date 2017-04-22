package com.techwolf.tedis.codec;

import java.io.Serializable;
import java.util.Map;

/**
 * Created by zhaoyalong on 17-3-27.
 */
public class MapWrapper implements Serializable {

    private static final long serialVersionUID = 9014506409060148872L;

    Map map;

    public Map getMap() {
        return map;
    }

    public void setMap(Map map) {
        this.map = map;
    }
}
