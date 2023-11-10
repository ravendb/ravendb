import React from "react";
import RavenConnectionString from "components/pages/database/settings/connectionStrings/forms/RavenConnectionString";
import SqlConnectionString from "components/pages/database/settings/connectionStrings/forms/SqlConnectionString";
import OlapConnectionString from "components/pages/database/settings/connectionStrings/forms/OlapConnectionString";
import ElasticSearchConnectionString from "components/pages/database/settings/connectionStrings/forms/ElasticSearchConnectionString";
import KafkaConnectionString from "components/pages/database/settings/connectionStrings/forms/KafkaConnectionString";
import RabbitMqConnectionString from "components/pages/database/settings/connectionStrings/forms/RabbitMqConnectionString";

interface RenderConnectionStringProps {
    type: string;
}

const RenderConnectionString = (props: RenderConnectionStringProps) => {
    const { type } = props;
    switch (type) {
        case "RavenDB":
            return <RavenConnectionString />;
        case "SQL":
            return <SqlConnectionString />;
        case "OLAP":
            return <OlapConnectionString />;
        case "ElasticSearch":
            return <ElasticSearchConnectionString />;
        case "Kafka":
            return <KafkaConnectionString />;
        case "RabbitMQ":
            return <RabbitMqConnectionString />;
        default:
            return null;
    }
};

export default RenderConnectionString;
