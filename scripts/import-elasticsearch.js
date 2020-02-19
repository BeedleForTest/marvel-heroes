const csv = require('csv-parser');
const fs = require('fs');

const { Client } = require('@elastic/elasticsearch')
const client = new Client({ node: 'http://localhost:9200' })
const indexName = 'heroes'

async function run () {

    await client.indices.delete({
      index: indexName,
      ignore_unavailable: true
    });

    // TODO il y a peut être des choses à faire ici avant de commencer ... 
    await client.indices.create({
        index: indexName, 
        body: {
            mappings: {
                properties: {
                    "suggest" : {
                        "type" : "completion"
                    },
                    "name": {
                        "type": "text",
                        "boost": 2
                    }
                }
            }
        } 
    }, (err, resp) => {
        if (err) console.trace(err.message);
    });

    let heroes = [];
    const BULK_SIZE = 50000;
    let totalDocumentsCreated = 0;

    // Read CSV file
    fs.createReadStream('all-heroes.csv')
        .pipe(csv({
            separator: ','
        }))

        .on('data', (data) => {
            // TODO ici on récupère les lignes du CSV ...
            heroes.push({
              index: {
                _id: data["id"]
              }
            });
            
            let aliasesTemp = data["aliases"].split(',');
            let partnersTemp = data["partners"].split(',');
            let secretIdentitiesTemp = data["secretIdentities"].split(',');
            heroes.push({
                name : data["name"],
                description : data["description"],
                imageUrl: data.imageUrl,
                secretIdentities : secretIdentitiesTemp,
                gender: data.gender,
                id: data.id,
                universe: data.universe,
                aliases : aliasesTemp,
                partners : partnersTemp,
                "suggest": [
                    {
                        "input": data["name"],
                        "weight" : 2
                    },
                    {
                        "input": aliasesTemp.concat(secretIdentitiesTemp),
                        "weight" : 1
                    }
                ]
            });
            
            if (heroes.length >= BULK_SIZE) {
              client.bulk({
                index: indexName,
                body: heroes
              }).then(response => {
                totalDocumentsCreated += response.body.items.length;
                console.log(totalDocumentsCreated + ' documents envoyés');
              }).catch(err => console.error(err));
              heroes = [];
            }
        })
        .on('end', () => {
            // TODO il y a peut être des choses à faire à la fin aussi !

          if (heroes.length != 0) {
            client.bulk({
              index: indexName,
              body: heroes
            }).then(response => {
              totalDocumentsCreated += response.body.items.length;
              console.log(totalDocumentsCreated + ' documents envoyés');
            }).catch(err => console.error(err));
          };
          client.close();
        });
        
}

run().catch(console.error);
