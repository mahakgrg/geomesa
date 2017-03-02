package org.locationtech.geomesa.hbase.filters;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Point;
import com.vividsolutions.jts.geom.Polygon;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.factory.Hints;
import org.geotools.filter.text.cql2.CQLException;
import org.geotools.filter.text.ecql.ECQL;
import org.geotools.util.Converters;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.index.conf.QueryHints;
import org.locationtech.geomesa.utils.geometry.Geometry;
import org.locationtech.geomesa.utils.geotools.GridSnap;
import org.locationtech.geomesa.utils.geotools.RichSimpleFeatureType;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import org.locationtech.geomesa.index.conf.QueryHints.RichHints;
import org.opengis.feature.type.GeometryDescriptor;
import org.opengis.filter.expression.Expression;
import scala.Option;
import scala.Tuple2;

/**
 * Created by mahak on 2/23/17.
 */
public class JLazyDensityFilter extends FilterBase{

    String sftString;
    SimpleFeatureType sft;
    KryoFeatureSerializer serializer;
    RichHints hints;
    private String ENVELOPE_OPT = "envelope";
    private String GRID_WIDTH_OPT  = "grid_width";
    private String GRID_HEIGHT_OPT  = "grid_height";
    private String WEIGHT_OPT   = "weight";

    int geomIndex = -1;
    // we snap each point into a pixel and aggregate based on that
    GridSnap gridSnap = null;

    Map<String, Object> options = new HashMap<>();

    //Because i dont know what type the map entry should be for the time being i am doing  STring x+"$" +y
    static Map<String, Double> DensityResult;

    int weightIndex = -2;

    public JLazyDensityFilter(String sftString, Hints hints) {
        this.sftString = sftString;
        configureSFT();
        configureOptions(hints);
        DensityResult = new HashMap<>();
    }

    public JLazyDensityFilter(SimpleFeatureType sft, Hints hints){//}, org.opengis.filter.Filter filter) {
        this.sft = sft;
        this.sftString = SimpleFeatureTypes.encodeType(sft, true);
        configureOptions(hints);
        DensityResult = new HashMap<>();
    }

    /**
     * Gets the weight for a feature from a double attribute
     */
    private double getWeightFromDouble(int i, SimpleFeature sf){
        Double d = (Double) sf.getAttribute(i);
        if (d == null){
            return 0.0;
        } else {
            return d;
        }
    }

    /**
     * Tries to convert a non-double attribute into a double
     */
    private double getWeightFromNonDouble(int i, SimpleFeature sf){
        Object d = sf.getAttribute(i);
        if (d == null) {
            return 0.0;
        } else {
            Double converted = Converters.convert(d, Double.class);
            if (converted == null){
                return 1.0;
            } else {
                return converted;
            }
        }
    }

    /**
     * Evaluates an arbitrary expression against the simple feature to return a weight
     */
    private double getWeightFromExpression(Expression e, SimpleFeature sf) {
        Double d = e.evaluate(sf, Double.class);
        if (d == null){
            return 0.0;
        } else {
            return d;
        }
    }

    /*
     * Returns the weight of a simple feature
     */
    double weightFn(SimpleFeature sf){
        double returnVal = 0.0;
        if(weightIndex == -2){
            returnVal =  1.0;
        }else if(weightIndex == -1){
            try {
                Expression expression = ECQL.toExpression((String) options.get(WEIGHT_OPT));
                returnVal = getWeightFromExpression(expression, sf);
            }
            catch(Exception e){
                System.out.println("Exception");
            }
        }else if(sft.getDescriptor(weightIndex).getType().getBinding() == Double.class){
            returnVal = getWeightFromDouble(weightIndex, sf);
        }else{
            returnVal = getWeightFromNonDouble(weightIndex, sf);
        }
        return returnVal;
    }

    private String getGeomField(){
        GeometryDescriptor gd = sft.getGeometryDescriptor();
        if (gd == null) {
            return null;
        } else {
            return gd.getLocalName();
        }
    }

    private int getGeomIndex(){
        return  sft.indexOf(getGeomField());
    }

    private Envelope getDensityEnvelope(Hints hints) {
        Envelope e = (Envelope) hints.get(QueryHints.DENSITY_BBOX());
        return e;
    }

    private int getDensityWidth(Hints hints){
        int width = (int )hints.get(QueryHints.DENSITY_WIDTH());
        return width;
    }

    private int getDensityHeight(Hints hints){
        int height = (int )hints.get(QueryHints.DENSITY_HEIGHT());
        return height;
    }

    private String getDensityWeight(Hints hints){
        String weight = (String)hints.get(QueryHints.DENSITY_WEIGHT());
        return weight;
    }

    public static Map<String, Double> getDensityResult(){
        return DensityResult;
    }
    private void configureOptions(Hints hints){
        geomIndex = getGeomIndex();
        if(hints == null) return;
        Envelope envelope = getDensityEnvelope(hints);
        int width = getDensityWidth(hints);
        int height = getDensityHeight(hints);
        String weight = getDensityWeight(hints);
        options.put(WEIGHT_OPT, weight);
        options.put(ENVELOPE_OPT, envelope);
        options.put(GRID_WIDTH_OPT, width);
        options.put(GRID_HEIGHT_OPT, height);
        gridSnap = new GridSnap(envelope, width, height);
        //val weightIndex = options.get(WEIGHT_OPT).map(sft.indexOf).getOrElse(-2)
        int index  = sft.indexOf((String)options.get(WEIGHT_OPT));
        weightIndex = index;
    }

    private void configureSFT() {
        sft = SimpleFeatureTypes.createType("QuickStart", sftString);
        serializer = new KryoFeatureSerializer(sft, SerializationOptions.withoutId());
    }

    boolean isPoints(){
       GeometryDescriptor gd = sft.getGeometryDescriptor();
       return (gd != null && gd.getType().getBinding() == Point.class);
    }

    /**
     * Writes a density record from a feature that has a point geometry
     */
    void writePoint(SimpleFeature sf, Double weight) {
        writePointToResult((Point)sf.getAttribute(geomIndex), weight);
    }
    /**
     * Writes a density record from a feature that has an arbitrary geometry
     */
    private void writeNonPoint(Geometry geom, Double weight){
        writePointToResult(safeCentroid(geom), weight);
    }

    private Point safeCentroid(Geometry geom){
        Polygon p = geom.noPolygon();
        Point centroid  = p.getCentroid();
        if(Double.isNaN(centroid.getCoordinate().x) || Double.isNaN(centroid.getCoordinate().y)){
            return p.getEnvelope().getCentroid();
        }else{
            return centroid;
        }
    }

    private void writePointToResult(Point pt, Double weight) {
        //TODO : SInce i am testing with hints as null, i can not configure gridsnap, so this does not work, so for the time being i dont use gridsnap
        //writeSnappedPoint(gridSnap.i(pt.getX()), gridSnap.j(pt.getY()), weight);
        writeSnappedPoint((int)pt.getX(), (int)pt.getY(), weight);
    }

    private void writeSnappedPoint(int x, int y, Double weight){
        String key = x+"$"+ y;
        DensityResult.put(key, DensityResult.getOrDefault(key, 0.0) + weight);
    }

    private void writePointToResult(Coordinate pt, Double weight){
        writeSnappedPoint(gridSnap.i(pt.x), gridSnap.j(pt.y), weight);
    }

    private void writePointToResult(double x, double y, double weight){
        writeSnappedPoint(gridSnap.i(x), gridSnap.j(y), weight);
    }

    @Override
    public ReturnCode filterKeyValue(Cell v) throws IOException {
        byte[] encodedSF = CellUtil.cloneValue(v);
        SimpleFeature sf = serializer.deserialize(encodedSF);

        System.out.println("In filter");
        if(isPoints()){
            writePoint(sf, weightFn(sf));
        }else{
            writeNonPoint((Geometry) sf.getDefaultGeometry(), weightFn(sf));
        }
        return ReturnCode.INCLUDE;
    }

    @Override
    public Cell transformCell(Cell v) throws IOException {
        byte[] encodedSF = CellUtil.cloneValue(v);
        SimpleFeature sf = serializer.deserialize(encodedSF);

        System.out.println("Who: " + sf.getAttribute("Who") + " When: " + sf.getAttribute("When") +  " Where  " + sf.getAttribute("Where"));

        return super.transformCell(v);
    }

    // TODO: Add static method to compute byte array from SFT and Filter.
    @Override
    public byte[] toByteArray() throws IOException {
        System.out.println("Serializing JLazyDensityFiler!");
        return Bytes.add(getLengthArray(sftString), convertToBytes(hints));
    }

    private byte[] convertToBytes(Object object) throws IOException {
        if(object == null){
            return Bytes.toBytes(0);
        }
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutput out = new ObjectOutputStream(bos)) {
            out.writeObject(object);
            byte[] arr =  bos.toByteArray();
            return Bytes.add(Bytes.toBytes(arr.length), arr);
        }
    }

    private static Object convertFromBytes(byte[] bytes) {
        if(bytes.length == 0){
            return null;
        }
        try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
             ObjectInput in = new ObjectInputStream(bis)) {
            return in.readObject();
        }
        catch(Exception e){
            return null;
        }
    }

    private byte[] getLengthArray(String s) {
        int len = getLen(s);
        if (len == 0) {
            return Bytes.toBytes(0);
        } else {
            return Bytes.add(Bytes.toBytes(len), s.getBytes());
        }
    }

    private int getLen(String s) {
        if (s != null) return s.length();
        else           return 0;
    }

    public static org.apache.hadoop.hbase.filter.Filter parseFrom(final byte [] pbBytes) throws DeserializationException {
        System.out.println("Creating JdensityFilter with parseFrom!");

        int sftLen =  Bytes.readAsInt(pbBytes, 0, 4);
        String sftString = new String(Bytes.copy(pbBytes, 4, sftLen));

        int hintsLen = Bytes.readAsInt(pbBytes, sftLen + 4, 4);
        Hints hints = (Hints) convertFromBytes(Bytes.copy(pbBytes, sftLen + 8, hintsLen));

        return new JLazyDensityFilter(sftString, hints);
    }
}
