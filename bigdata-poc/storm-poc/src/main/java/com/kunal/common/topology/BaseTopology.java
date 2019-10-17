package com.kunal.common.topology;

import com.kunal.common.config.StormConfiguration;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.utils.Utils;

public abstract class BaseTopology {

    private boolean local;
    public void start(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {

        if(args.length<2){
            System.out.println(" Missing Arguments .....");
            System.exit(0);
        }

        StormConfiguration conf=new StormConfiguration();
        StormTopology topologyBuilder=getTopology(conf);

        if(!local){
            StormSubmitter.submitTopology(args[0],conf,topologyBuilder);
        }else {
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology("test-topology",conf,topologyBuilder);
            Utils.sleep(1000);
            cluster.killTopology("test-topology");
            cluster.shutdown();
        }
    }

    protected abstract StormTopology getTopology(StormConfiguration conf);
}
