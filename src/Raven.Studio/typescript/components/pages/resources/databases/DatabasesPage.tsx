import React, { useCallback, useEffect, useMemo, useReducer, useState } from "react";
import { useServices } from "hooks/useServices";
import { databasesStatsReducer, databasesStatsReducerInitializer, DatabasesStatsState } from "./DatabasesStatsReducer";
import { DatabasePanel } from "./DatabasePanel";
import { DatabasesToolbarActions } from "./DatabasesToolbarActions";
import { DatabasesFilter } from "./DatabasesFilter";
import { DatabasesCounter } from "./DatabasesCounter";
import { NoDatabases } from "./NoDatabases";
import { DatabaseFilterCriteria, DatabaseSharedInfo } from "../../../models/databases";
import { useChanges } from "hooks/useChanges";
import {
    Button,
    Card,
    CardBody,
    CardLink,
    CardText,
    CardTitle,
    Carousel,
    CarouselCaption,
    CarouselControl,
    CarouselIndicators,
    CarouselItem,
    Col,
    Dropdown,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Form,
    FormGroup,
    FormText,
    Input,
    Label,
    ListGroup,
    ListGroupItem,
    Modal,
    ModalBody,
    ModalFooter,
    ModalHeader,
    Table,
    Toast,
    ToastBody,
    ToastHeader,
} from "reactstrap";

interface DatabasesPageProps {
    activeDatabase?: string;
}

const items = [
    {
        src: "https://picsum.photos/id/123/1200/400",
        altText: "Slide 1",
        caption: "Slide 1",
        key: 1,
    },
    {
        src: "https://picsum.photos/id/456/1200/400",
        altText: "Slide 2",
        caption: "Slide 2",
        key: 2,
    },
    {
        src: "https://picsum.photos/id/678/1200/400",
        altText: "Slide 3",
        caption: "Slide 3",
        key: 3,
    },
];

function ModalUsage(args: any) {
    const [modal, setModal] = useState(false);

    const toggle = () => setModal(!modal);

    return (
        <div>
            <Button color="danger" onClick={toggle}>
                Click Me
            </Button>
            <Modal wrapClassName="bs5" fade isOpen={modal} toggle={toggle} {...args}>
                <ModalHeader toggle={toggle}>Modal title</ModalHeader>
                <ModalBody>
                    Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore
                    et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut
                    aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse
                    cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in
                    culpa qui officia deserunt mollit anim id est laborum.
                </ModalBody>
                <ModalFooter>
                    <Button color="primary" onClick={toggle}>
                        Do Something
                    </Button>{" "}
                    <Button color="secondary" onClick={toggle}>
                        Cancel
                    </Button>
                </ModalFooter>
            </Modal>
        </div>
    );
}

function Carousel2(args: any) {
    const [activeIndex, setActiveIndex] = useState(0);
    const [animating, setAnimating] = useState(false);

    const next = () => {
        if (animating) return;
        const nextIndex = activeIndex === items.length - 1 ? 0 : activeIndex + 1;
        setActiveIndex(nextIndex);
    };

    const previous = () => {
        if (animating) return;
        const nextIndex = activeIndex === 0 ? items.length - 1 : activeIndex - 1;
        setActiveIndex(nextIndex);
    };

    const goToIndex = (newIndex: any) => {
        if (animating) return;
        setActiveIndex(newIndex);
    };

    const slides = items.map((item) => {
        return (
            <CarouselItem onExiting={() => setAnimating(true)} onExited={() => setAnimating(false)} key={item.src}>
                <img src={item.src} alt={item.altText} />
                <CarouselCaption captionText={item.caption} captionHeader={item.caption} />
            </CarouselItem>
        );
    });

    return (
        <Carousel activeIndex={activeIndex} next={next} previous={previous} {...args}>
            <CarouselIndicators items={items} activeIndex={activeIndex} onClickHandler={goToIndex} />
            {slides}
            <CarouselControl direction="prev" directionText="Previous" onClickHandler={previous} />
            <CarouselControl direction="next" directionText="Next" onClickHandler={next} />
        </Carousel>
    );
}

function Example({ direction, ...args }: any) {
    const [dropdownOpen, setDropdownOpen] = useState(false);

    const toggle = () => setDropdownOpen((prevState) => !prevState);

    return (
        <div className="d-flex p-5">
            <Dropdown isOpen={dropdownOpen} toggle={toggle} direction={direction}>
                <DropdownToggle caret>Dropdown</DropdownToggle>
                <DropdownMenu {...args}>
                    <DropdownItem header>Header</DropdownItem>
                    <DropdownItem>Some Action</DropdownItem>
                    <DropdownItem text>Dropdown Item Text</DropdownItem>
                    <DropdownItem disabled>Action (disabled)</DropdownItem>
                    <DropdownItem divider />
                    <DropdownItem>Foo Action</DropdownItem>
                    <DropdownItem>Bar Action</DropdownItem>
                    <DropdownItem>Quo Action</DropdownItem>
                </DropdownMenu>
            </Dropdown>
        </div>
    );
}

function filterDatabases(stats: DatabasesStatsState, criteria: DatabaseFilterCriteria) {
    if (criteria.searchText) {
        return stats.databases.filter((x) => x.name.toLowerCase().includes(criteria.searchText.toLowerCase()));
    }

    return stats.databases;
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
export function DatabasesPage(props: DatabasesPageProps) {
    //TODO: highlight active database

    const [stats, dispatch] = useReducer(databasesStatsReducer, null, databasesStatsReducerInitializer);

    const [filter, setFilter] = useState<DatabaseFilterCriteria>(() => ({
        searchText: "",
    }));

    const { serverNotifications } = useChanges();

    const [selectedDatabases, setSelectedDatabases] = useState<string[]>([]);

    const { databasesService } = useServices();

    const filteredDatabases = useMemo(() => {
        //TODO: filter and sort databases
        //TODO: update selection if needed
        return filterDatabases(stats, filter);
    }, [filter, stats]);

    const fetchDatabases = useCallback(async () => {
        const stats = await databasesService.getDatabases();

        dispatch({
            type: "StatsLoaded",
            stats,
        });
    }, [databasesService]);

    const toggleSelectAll = useCallback(() => {
        const selectedCount = selectedDatabases.length;

        if (selectedCount > 0) {
            setSelectedDatabases([]);
        } else {
            setSelectedDatabases(filteredDatabases.map((x) => x.name));
        }
    }, [selectedDatabases, filteredDatabases]);

    const databasesSelectionState = useMemo<checkbox>(() => {
        const selectedCount = selectedDatabases.length;
        const dbsCount = filteredDatabases.length;
        if (stats.databases && dbsCount === selectedCount) {
            return "checked";
        }

        if (selectedCount > 0) {
            return "some_checked";
        }

        return "unchecked";
    }, [filteredDatabases, selectedDatabases, stats.databases]);

    const toggleSelection = (db: DatabaseSharedInfo) => {
        if (selectedDatabases.includes(db.name)) {
            setSelectedDatabases((s) => s.filter((x) => x !== db.name));
        } else {
            setSelectedDatabases((s) => s.concat(db.name));
        }
    };

    useEffect(() => {
        fetchDatabases();
    }, [fetchDatabases]);

    useEffect(() => {
        if (serverNotifications) {
            const sub = serverNotifications.watchAllDatabaseChanges(() => fetchDatabases());

            return () => sub.off();
        }
    }, [serverNotifications, fetchDatabases]);

    return (
        <div>
            <div className="bs5">
                <Example />
                <Carousel2 />
                <ModalUsage />
                <Table>
                    <thead>
                        <tr>
                            <th>Class</th>
                            <th>Heading</th>
                            <th>Heading</th>
                        </tr>
                    </thead>
                    <tbody>
                        <tr className="table-primary">
                            <td>primary</td>
                            <td>Cell</td>
                            <td>Cell</td>
                        </tr>
                        <tr className="table-secondary">
                            <td>secondary</td>
                            <td>Cell</td>
                            <td>Cell</td>
                        </tr>
                        <tr className="table-success">
                            <td>success</td>
                            <td>Cell</td>
                            <td>Cell</td>
                        </tr>
                        <tr className="table-danger">
                            <td>danger</td>
                            <td>Cell</td>
                            <td>Cell</td>
                        </tr>
                        <tr className="table-warning">
                            <td>warning</td>
                            <td>Cell</td>
                            <td>Cell</td>
                        </tr>
                        <tr className="table-info">
                            <td>info</td>
                            <td>Cell</td>
                            <td>Cell</td>
                        </tr>
                        <tr className="table-light">
                            <td>light</td>
                            <td>Cell</td>
                            <td>Cell</td>
                        </tr>
                        <tr className="table-dark">
                            <td>dark</td>
                            <td>Cell</td>
                            <td>Cell</td>
                        </tr>
                    </tbody>
                </Table>

                <Card
                    style={{
                        width: "18rem",
                    }}
                >
                    <img alt="Card" src="https://picsum.photos/300/200" />
                    <CardBody>
                        <CardTitle tag="h5">Card Title</CardTitle>
                        <CardText>This is some text within a card body.</CardText>
                    </CardBody>
                    <ListGroup flush>
                        <ListGroupItem>An item</ListGroupItem>
                        <ListGroupItem>A second item</ListGroupItem>
                        <ListGroupItem>And a third item</ListGroupItem>
                    </ListGroup>
                    <CardBody>
                        <CardLink href="#">Card Link</CardLink>
                        <CardLink href="#">Another Card Link</CardLink>
                    </CardBody>
                </Card>

                <Button type="button" color="primary">
                    TEST
                </Button>
                <Form>
                    <FormGroup row>
                        <Label for="exampleEmail" sm={2}>
                            Email
                        </Label>
                        <Col sm={10}>
                            <Input id="exampleEmail" name="email" placeholder="with a placeholder" type="email" />
                        </Col>
                    </FormGroup>
                    <FormGroup row>
                        <Label for="examplePassword" sm={2}>
                            Password
                        </Label>
                        <Col sm={10}>
                            <Input
                                id="examplePassword"
                                name="password"
                                placeholder="password placeholder"
                                type="password"
                            />
                        </Col>
                    </FormGroup>
                    <FormGroup row>
                        <Label for="exampleSelect" sm={2}>
                            Select
                        </Label>
                        <Col sm={10}>
                            <Input id="exampleSelect" name="select" type="select">
                                <option>1</option>
                                <option>2</option>
                                <option>3</option>
                                <option>4</option>
                                <option>5</option>
                            </Input>
                        </Col>
                    </FormGroup>
                    <FormGroup row>
                        <Label for="exampleSelectMulti" sm={2}>
                            Select Multiple
                        </Label>
                        <Col sm={10}>
                            <Input id="exampleSelectMulti" multiple name="selectMulti" type="select">
                                <option>1</option>
                                <option>2</option>
                                <option>3</option>
                                <option>4</option>
                                <option>5</option>
                            </Input>
                        </Col>
                    </FormGroup>
                    <FormGroup row>
                        <Label for="exampleText" sm={2}>
                            Text Area
                        </Label>
                        <Col sm={10}>
                            <Input id="exampleText" name="text" type="textarea" />
                        </Col>
                    </FormGroup>
                    <FormGroup row>
                        <Label for="exampleFile" sm={2}>
                            File
                        </Label>
                        <Col sm={10}>
                            <Input id="exampleFile" name="file" type="file" />
                            <FormText>
                                This is some placeholder block-level help text for the above input. It‘s a bit lighter
                                and easily wraps to a new line.
                            </FormText>
                        </Col>
                    </FormGroup>
                    <FormGroup row tag="fieldset">
                        <legend className="col-form-label col-sm-2">Radio Buttons</legend>
                        <Col sm={10}>
                            <FormGroup check>
                                <Input name="radio2" type="radio" />{" "}
                                <Label check>Option one is this and that—be sure to include why it‘s great</Label>
                            </FormGroup>
                            <FormGroup check>
                                <Input name="radio2" type="radio" />{" "}
                                <Label check>
                                    Option two can be something else and selecting it will deselect option one
                                </Label>
                            </FormGroup>
                            <FormGroup check disabled>
                                <Input disabled name="radio2" type="radio" />{" "}
                                <Label check>Option three is disabled</Label>
                            </FormGroup>
                        </Col>
                    </FormGroup>
                    <FormGroup row>
                        <Label for="checkbox2" sm={2}>
                            Checkbox
                        </Label>
                        <Col
                            sm={{
                                size: 10,
                            }}
                        >
                            <FormGroup check>
                                <Input id="checkbox2" type="checkbox" /> <Label check>Check me out</Label>
                            </FormGroup>
                        </Col>
                    </FormGroup>
                    <FormGroup check row>
                        <Col
                            sm={{
                                offset: 2,
                                size: 10,
                            }}
                        >
                            <Button>Submit</Button>
                        </Col>
                    </FormGroup>
                </Form>
            </div>
            <div className="flex-header">
                <div className="databasesToolbar">
                    <DatabasesToolbarActions />
                    <DatabasesFilter
                        filter={filter}
                        setFilter={setFilter}
                        selectionState={databasesSelectionState}
                        toggleSelectAll={toggleSelectAll}
                    />
                </div>
            </div>
            <div className="flex-grow scroll js-scroll-container">
                <DatabasesCounter />
                <div>
                    {filteredDatabases.map((db) => (
                        <DatabasePanel
                            key={db.name}
                            selected={selectedDatabases.includes(db.name)}
                            toggleSelection={() => toggleSelection(db)}
                            db={db}
                        />
                    ))}

                    {!stats.databases.length && <NoDatabases />}
                </div>
            </div>
        </div>
    );
}
