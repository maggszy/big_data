# Spark cluster

## How to use that

1. Add your pySpark code to the `src` folder (the current content is a placeholder app)
2. Download Hamachi and the contents of this directory to all machines you want to be in the cluster
3. On a **master** machine create a Hamachi server
4. Change the `.env` file to your master IP in the Hamachi network
5. Configure firewall settings on all the machines to allow Hamachi connections (eg. on Windows you should usually make sure that the checkboxes presented in this video https://www.youtube.com/watch?v=wsiL8_iTbK are not selected)
6. Connect all the machines to the Hamachi VPN
7. Start the master node on a **master** machine:

    ```sh
    docker compose -f docker-compose-master.yaml up
    ```

8. When completed run the worker nodes on all the **worker** machines and the **master** machine too (if you want):

    ```sh
    docker compose -f docker-compose-worker.yaml up
    ```

9. When all the nodes are connected use `run-app.sh` script on the **master** machine to run your app
  