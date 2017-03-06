package org.locationtech.geomesa.hbase.filters;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.CoprocessorException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.geotools.factory.Hints;
import org.locationtech.geomesa.features.interop.SerializationOptions;
import org.locationtech.geomesa.features.kryo.KryoFeatureSerializer;
import org.locationtech.geomesa.hbase.filters.Density.*;
import org.locationtech.geomesa.utils.geotools.SimpleFeatureTypes;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DensityCoprocessor extends DensityService implements Coprocessor, CoprocessorService {

    private RegionCoprocessorEnvironment env;

    SimpleFeatureType sft = null;
    Hints hints = null;
    DensityCoprocessor(SimpleFeatureType sft, Hints hints){
        this.sft = sft;
        this.hints = hints;
    }

    @Override
    public void start(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        if (env instanceof RegionCoprocessorEnvironment) {
            this.env = (RegionCoprocessorEnvironment) env;
        } else {
            throw new CoprocessorException("Must be loaded on a table region!");
        }
    }

    @Override
    public void stop(CoprocessorEnvironment coprocessorEnvironment) throws IOException {
        //do nothing
    }

    @Override
    public Service getService() {
        return this;
    }

    @Override
    public void getDensity(RpcController controller, DensityRequest request, RpcCallback<DensityResponse> done) {
        SimpleFeatureType outputsft = SimpleFeatureTypes.createType("result", "mapkey:String,weight:java.lang.Double");
        KryoFeatureSerializer output_serializer = new KryoFeatureSerializer(outputsft, SerializationOptions.withoutId());
        System.out.println(request.getFamily());
        JLazyDensityFilter filter = new JLazyDensityFilter(request.getFamily(), null);
        Scan scan = new Scan();
        scan.setFilter(filter);
        //scan.addFamily(Bytes.toBytes(request.getFamily()));
        //scan.addColumn(Bytes.toBytes(request.getFamily()), Bytes.toBytes(request.getColumn()));
        DensityResponse response = null;
        InternalScanner scanner = null;
        try {
            scanner = env.getRegion().getScanner(scan);
            List<Cell> results = new ArrayList();
            boolean hasMore = false;
            Map<String, Double> resultMap = new HashMap<>();
            do {
                hasMore = scanner.next(results);
                for (Cell cell : results) {
                    byte[] row  = CellUtil.cloneRow(cell);
                    byte[] encodedSF = CellUtil.cloneValue(cell);
                    SimpleFeature sf = output_serializer.deserialize(encodedSF);
                    String key = (String) sf.getAttribute("mapkey");
                    Double value = (Double) sf.getAttribute("weight");
                    resultMap.put(key, resultMap.getOrDefault(key, 0.0) + value);
                }
                results.clear();
            } while (hasMore);
            List<Pair>  pairs = new ArrayList<>();
            resultMap.forEach((String key, Double val) -> {
                Pair pair = Pair.newBuilder().setKey(key).setValue(val).build();
                pairs.add(pair);
            });

            response = DensityResponse.newBuilder().addAllPairs(pairs).build();
//            response = DensityResponse.newBuilder().setSum(sum).build();

        } catch (IOException ioe) {
            ResponseConverter.setControllerException(controller, ioe);
        } finally {
            if (scanner != null) {
                try {
                    scanner.close();
                } catch (IOException ignored) {
                }
            }
        }
        done.run(response);
    }
}