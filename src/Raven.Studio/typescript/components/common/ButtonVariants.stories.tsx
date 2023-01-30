import { ComponentMeta } from "@storybook/react";
import React from "react";
import { withBootstrap5, withStorybookContexts } from "test/storybookTestUtils";
import { Button, Col, Row } from "reactstrap";
export default {
    title: "Bits/Buttons",
    decorators: [withStorybookContexts, withBootstrap5],
    component: Button,
} as ComponentMeta<typeof Button>;

export function Variants() {
    return (
        <>
            <Row>
                default size
                <Col className="d-flex gap-1">
                    <Button color="primary">primary</Button>
                    <Button color="secondary">secondary</Button>
                    <Button color="danger">danger</Button>
                    <Button color="dark">dark</Button>
                    <Button color="info">info</Button>
                    <Button color="light">light</Button>
                    <Button color="progress">progress</Button>
                    <Button color="success">success</Button>
                    <Button color="warning">warning</Button>
                    <Button color="node">node</Button>
                    <Button color="shard">shard</Button>
                </Col>
            </Row>
            <Row className="mt-3">
                default size, outline
                <Col className="d-flex gap-1">
                    <Button color="primary" outline>
                        primary
                    </Button>
                    <Button color="secondary" outline>
                        secondary
                    </Button>
                    <Button color="danger" outline>
                        danger
                    </Button>
                    <Button color="dark" outline>
                        dark
                    </Button>
                    <Button color="info" outline>
                        info
                    </Button>
                    <Button color="light" outline>
                        light
                    </Button>
                    <Button color="progress" outline>
                        progress
                    </Button>
                    <Button color="success" outline>
                        success
                    </Button>
                    <Button color="warning" outline>
                        warning
                    </Button>
                    <Button color="node" outline>
                        node
                    </Button>
                    <Button color="shard" outline>
                        shard
                    </Button>
                </Col>
            </Row>
            <Row className="mt-3">
                xs size
                <Col className="d-flex gap-1">
                    <Button color="primary" size="xs">
                        primary
                    </Button>
                    <Button color="secondary" size="xs">
                        secondary
                    </Button>
                    <Button color="danger" size="xs">
                        danger
                    </Button>
                    <Button color="dark" size="xs">
                        dark
                    </Button>
                    <Button color="info" size="xs">
                        info
                    </Button>
                    <Button color="light" size="xs">
                        light
                    </Button>
                    <Button color="progress" size="xs">
                        progress
                    </Button>
                    <Button color="success" size="xs">
                        success
                    </Button>
                    <Button color="warning" size="xs">
                        warning
                    </Button>
                    <Button color="node" size="xs">
                        node
                    </Button>
                    <Button color="shard" size="xs">
                        shard
                    </Button>
                </Col>
            </Row>
            <Row className="mt-3">
                xs size, outline
                <Col className="d-flex gap-1">
                    <Button color="primary" size="xs" outline>
                        primary
                    </Button>
                    <Button color="secondary" size="xs" outline>
                        secondary
                    </Button>
                    <Button color="danger" size="xs" outline>
                        danger
                    </Button>
                    <Button color="dark" size="xs" outline>
                        dark
                    </Button>
                    <Button color="info" size="xs" outline>
                        info
                    </Button>
                    <Button color="light" size="xs" outline>
                        light
                    </Button>
                    <Button color="progress" size="xs" outline>
                        progress
                    </Button>
                    <Button color="success" size="xs" outline>
                        success
                    </Button>
                    <Button color="warning" size="xs" outline>
                        warning
                    </Button>
                    <Button color="node" size="xs" outline>
                        node
                    </Button>
                    <Button color="shard" size="xs" outline>
                        shard
                    </Button>
                </Col>
            </Row>
        </>
    );
}
