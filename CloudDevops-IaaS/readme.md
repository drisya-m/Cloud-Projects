Automate the service setup used in Anomaly detection Project(CloudManagedServices) using CloudFormation and CloudDeploy. Instead of API we use raw_data.py to
generate the input data.

The folder structure for artifacts:

-- temperatureanomaly.zip

|

	-- appspec.yml

	|

	-- scripts

	|

		--

 		|

  		-- pre-install.sh
	
 		|

  		-- post-install.sh

 		|

  		-- temperatureanomaly-start.sh

 		|

  		-- temperatureanomaly-stop.sh


	-- backend.zip

    	|

     		-- raw_data.py

    		|

     		-- requirements.txt
