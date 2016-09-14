# kafka-example-app

An example application to be used with the [example Kafka BOSH release](https://github.com/pivotal-cf-experimental/kafka-example-service-release)

---

To write data: `curl -XPOST http://kafka-example-app.com/queues/a-queue -d 'data'`

To get data: `curl  http://kafka-example-app.com/queues/a-queue`

**Please note that this release is meant for demonstration purposes only, not for production use.**

---

README - PIVOTAL SDK - MODIFIABLE CODE NOTICE 

The contents of this GitHub repository available at https://github.com/pivotal-cf-experimental/kafka-example-app are licensed to you 
under the terms of the Pivotal Software Development Kit License Agreement ("SDK EULA") 
and are designated by Pivotal Software, Inc. as "Modifiable Code."

Your rights to distribute, modify or create derivative works of all or portions of this 
Modifiable Code are described in the Pivotal Software Development Kit License Agreement 
("SDK EULA") and this Modifiable Code may only be used in accordance with the terms and
conditions of the SDK EULA.

Unless required by applicable law or agreed to in writing, this Modifiable Code is 
provided on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either 
express or implied. See the SDK EULA for the specific language governing permissions and
limitations for this Modifiable Code under the SDK EULA. 
