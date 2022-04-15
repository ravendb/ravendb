import { IndexesPage } from "./IndexesPage";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import nonShardedDatabase from "models/resources/nonShardedDatabase";
import React from "react";


export default {
    title: "Indexes page",
    component: IndexesPage
} as ComponentMeta<typeof IndexesPage>;

export const Story1: ComponentStory<typeof IndexesPage> = () => {
    const db = new nonShardedDatabase({
        Name: "db1"
    } as any, ko.observable("A"));

    return (

        <div className="indexes content-margin no-transition absolute-fill" style={{ margin: "50px" }}>
            <IndexesPage database={db}/>
        </div>

    );
}
