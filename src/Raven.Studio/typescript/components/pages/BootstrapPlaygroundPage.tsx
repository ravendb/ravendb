/* eslint-disable local-rules/no-reactstrap-alert */
import React, { useState } from "react";
import {
    Alert,
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
    Accordion,
    AccordionBody,
    AccordionHeader,
    AccordionItem,
    Tooltip,
    Popover,
    PopoverHeader,
    PopoverBody,
    Offcanvas,
    OffcanvasBody,
    OffcanvasHeader,
} from "reactstrap";

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

    const [tooltipOpen, setTooltipOpen] = useState(false);
    const toggleTooltip = () => setTooltipOpen(!tooltipOpen);

    return (
        <div>
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

                <Dropdown isOpen={dropdownOpen} color="dark" toggle={toggle} direction={direction}>
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

            <div>
                <h1>Tooltip</h1>
                <p>
                    Somewhere in here is a{" "}
                    <a href="https://example.com" target="_blank" rel="noreferrer" id="TooltipExample">
                        tooltip
                    </a>
                    .
                </p>
                <Tooltip {...args} isOpen={tooltipOpen} target="TooltipExample" toggle={toggleTooltip} className="bs5">
                    Hello world!
                </Tooltip>
            </div>
            <div className="margin-bottom">
                <h1>Popover</h1>
                <Button id="Popover1" type="button">
                    Launch Popover
                </Button>
                <Popover flip target="Popover1" className="bs5">
                    <PopoverHeader>Popover Title</PopoverHeader>
                    <PopoverBody>
                        Sed posuere consectetur est at lobortis. Aenean eu leo quam. Pellentesque ornare sem lacinia
                        quam venenatis vestibulum.
                    </PopoverBody>
                </Popover>
            </div>
            <div className="margin-bottom">
                <div>
                    <Button color="primary">Open Offcanvas</Button>
                    <Offcanvas>
                        <OffcanvasHeader>Offcanvas</OffcanvasHeader>
                        <OffcanvasBody>
                            <strong>This is the Offcanvas body.</strong>
                        </OffcanvasBody>
                    </Offcanvas>
                </div>
            </div>
        </div>
    );
}

function TableExample() {
    return (
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
    );
}

function FormExample() {
    return (
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
                    <Input id="examplePassword" name="password" placeholder="password placeholder" type="password" />
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
                        This is some placeholder block-level help text for the above input. It‘s a bit lighter and
                        easily wraps to a new line.
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
                        <Label check>Option two can be something else and selecting it will deselect option one</Label>
                    </FormGroup>
                    <FormGroup check disabled>
                        <Input disabled name="radio2" type="radio" /> <Label check>Option three is disabled</Label>
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
    );
}

function CardExample() {
    return (
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
    );
}

function AlertExample() {
    return (
        <div>
            <Alert color="primary">This is a primary alert — check it out!</Alert>
            <Alert color="secondary">This is a primary alert — check it out!</Alert>
            <Alert>This is a primary alert — check it out!</Alert>
            <Alert color="danger">This is a primary alert — check it out!</Alert>
            <Alert color="warning">This is a primary alert — check it out!</Alert>
            <Alert color="info">This is a primary alert — check it out!</Alert>
            <Alert color="light">This is a primary alert — check it out!</Alert>
            <Alert color="dark">This is a primary alert — check it out!</Alert>
        </div>
    );
}

function AccordionExample() {
    const [open, setOpen] = useState("some-id");

    return (
        <div>
            <Accordion open={open} toggle={() => setOpen(open ? null : "some-id")}>
                <AccordionItem>
                    <AccordionHeader targetId="1">Accordion Item 1</AccordionHeader>
                    <AccordionBody accordionId="1">
                        <strong>This is the first item&#39;s accordion body.</strong>
                        You can modify any of this with custom CSS or overriding our default variables. It&#39;s also
                        worth noting that just about any HTML can go within the <code>.accordion-body</code>, though the
                        transition does limit overflow.
                    </AccordionBody>
                </AccordionItem>
                <AccordionItem>
                    <AccordionHeader targetId="2">Accordion Item 2</AccordionHeader>
                    <AccordionBody accordionId="2">
                        <strong>This is the second item&#39;s accordion body.</strong>
                        You can modify any of this with custom CSS or overriding our default variables. It&#39;s also
                        worth noting that just about any HTML can go within the <code>.accordion-body</code>, though the
                        transition does limit overflow.
                    </AccordionBody>
                </AccordionItem>
                <AccordionItem>
                    <AccordionHeader targetId="3">Accordion Item 3</AccordionHeader>
                    <AccordionBody accordionId="3">
                        <strong>This is the third item&#39;s accordion body.</strong>
                        You can modify any of this with custom CSS or overriding our default variables. It&#39;s also
                        worth noting that just about any HTML can go within the <code>.accordion-body</code>, though the
                        transition does limit overflow.
                    </AccordionBody>
                </AccordionItem>
            </Accordion>
        </div>
    );
}

export function BootstrapPlaygroundPage() {
    const myHTML = `<h1>Range</h1>
<label for="disabledRange" class="form-label">Disabled range</label>
<input type="range" class="form-range" id="disabledRange" disabled>
<label for="customRange2" class="form-label">Example range</label>
<input type="range" class="form-range" min="0" max="5" id="customRange2">

<h1>Tabs</h1>
<ul class="nav nav-tabs">
  <li class="nav-item">
    <a class="nav-link active" aria-current="page" href="#">Active</a>
  </li>
  <li class="nav-item">
    <a class="nav-link" href="#">Link</a>
  </li>
  <li class="nav-item">
    <a class="nav-link" href="#">Link</a>
  </li>
  <li class="nav-item">
    <a class="nav-link disabled" href="#" tabindex="-1" aria-disabled="true">Disabled</a>
  </li>
</ul>

<ul class="nav nav-pills margin-top">
  <li class="nav-item">
    <a class="nav-link active" aria-current="page" href="#">Active</a>
  </li>
  <li class="nav-item">
    <a class="nav-link" href="#">Link</a>
  </li>
  <li class="nav-item">
    <a class="nav-link" href="#">Link</a>
  </li>
  <li class="nav-item">
    <a class="nav-link disabled" href="#" tabindex="-1" aria-disabled="true">Disabled</a>
  </li>
</ul>
<h1>Pagination</h1>
<nav aria-label="...">
  <ul class="pagination">
    <li class="page-item disabled">
      <a class="page-link" href="#" tabindex="-1" aria-disabled="true">Previous</a>
    </li>
    <li class="page-item"><a class="page-link" href="#">1</a></li>
    <li class="page-item active" aria-current="page">
      <a class="page-link" href="#">2</a>
    </li>
    <li class="page-item"><a class="page-link" href="#">3</a></li>
    <li class="page-item">
      <a class="page-link" href="#">Next</a>
    </li>
  </ul>
</nav>
<h1>Card</h1>
<div class="card" style="width: 18rem;">
  <img src="..." class="card-img-top" alt="...">
  <div class="card-body">
    <h5 class="card-title">Card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
    <a href="#" class="btn btn-primary">Go somewhere</a>
  </div>
</div>

<div class="card" style="width: 18rem;">
  <div class="card-body">
    <h5 class="card-title">Card title</h5>
    <h6 class="card-subtitle mb-2 text-muted">Card subtitle</h6>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
    <a href="#" class="card-link">Card link</a>
    <a href="#" class="card-link">Another link</a>
  </div>
</div>

<div class="card" style="width: 18rem;">
  <img src="..." class="card-img-top" alt="...">
  <div class="card-body">
    <h5 class="card-title">Card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
  <ul class="list-group list-group-flush">
    <li class="list-group-item">An item</li>
    <li class="list-group-item">A second item</li>
    <li class="list-group-item">A third item</li>
  </ul>
  <div class="card-body">
    <a href="#" class="card-link">Card link</a>
    <a href="#" class="card-link">Another link</a>
  </div>
</div>

<div class="card">
  <div class="card-header">
    Featured
  </div>
  <div class="card-body">
    <h5 class="card-title">Special title treatment</h5>
    <p class="card-text">With supporting text below as a natural lead-in to additional content.</p>
    <a href="#" class="btn btn-primary">Go somewhere</a>
  </div>
</div>

<div class="card">
  <div class="card-header">
    Quote
  </div>
  <div class="card-body">
    <blockquote class="blockquote mb-0">
      <p>A well-known quote, contained in a blockquote element.</p>
      <footer class="blockquote-footer">Someone famous in <cite title="Source Title">Source Title</cite></footer>
    </blockquote>
  </div>
</div>

<div class="card text-center">
  <div class="card-header">
    Featured
  </div>
  <div class="card-body">
    <h5 class="card-title">Special title treatment</h5>
    <p class="card-text">With supporting text below as a natural lead-in to additional content.</p>
    <a href="#" class="btn btn-primary">Go somewhere</a>
  </div>
  <div class="card-footer text-muted">
    2 days ago
  </div>
</div>

<div class="card text-center">
  <div class="card-header">
    <ul class="nav nav-tabs card-header-tabs">
      <li class="nav-item">
        <a class="nav-link active" aria-current="true" href="#">Active</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" href="#">Link</a>
      </li>
      <li class="nav-item">
        <a class="nav-link disabled" href="#" tabindex="-1" aria-disabled="true">Disabled</a>
      </li>
    </ul>
  </div>
  <div class="card-body">
    <h5 class="card-title">Special title treatment</h5>
    <p class="card-text">With supporting text below as a natural lead-in to additional content.</p>
    <a href="#" class="btn btn-primary">Go somewhere</a>
  </div>
</div>

<div class="card text-white bg-primary mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Primary card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card text-white bg-secondary mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Secondary card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card text-white bg-success mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Success card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card text-white bg-danger mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Danger card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card text-dark bg-warning mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Warning card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card text-dark bg-info mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Info card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card text-dark bg-light mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Light card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card text-white bg-dark mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Dark card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>

<div class="card border-primary mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body text-primary">
    <h5 class="card-title">Primary card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card border-secondary mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body text-secondary">
    <h5 class="card-title">Secondary card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card border-success mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body text-success">
    <h5 class="card-title">Success card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card border-danger mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body text-danger">
    <h5 class="card-title">Danger card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card border-warning mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Warning card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card border-info mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Info card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card border-light mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body">
    <h5 class="card-title">Light card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<div class="card border-dark mb-3" style="max-width: 18rem;">
  <div class="card-header">Header</div>
  <div class="card-body text-dark">
    <h5 class="card-title">Dark card title</h5>
    <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
  </div>
</div>
<h1>Badges</h1>
<div class="bs5">
<span class="badge bg-primary">Primary</span>
<span class="badge bg-secondary">Secondary</span>
<span class="badge bg-success">Success</span>
<span class="badge bg-danger">Danger</span>
<span class="badge bg-warning text-dark">Warning</span>
<span class="badge bg-info text-dark">Info</span>
<span class="badge bg-light text-dark">Light</span>
<span class="badge bg-dark">Dark</span>

<br/>
<span class="badge rounded-pill bg-primary">Primary</span>
<span class="badge rounded-pill bg-secondary">Secondary</span>
<span class="badge rounded-pill bg-success">Success</span>
<span class="badge rounded-pill bg-danger">Danger</span>
<span class="badge rounded-pill bg-warning text-dark">Warning</span>
<span class="badge rounded-pill bg-info text-dark">Info</span>
<span class="badge rounded-pill bg-light text-dark">Light</span>
<span class="badge rounded-pill bg-dark">Dark</span>
</div>

<h1>Alert</h1>


<div class="alert alert-primary" role="alert">
  A simple primary alert with <a href="#" class="alert-link">an example link</a>. Give it a click if you like.
</div>
<div class="alert alert-secondary" role="alert">
  A simple secondary alert with <a href="#" class="alert-link">an example link</a>. Give it a click if you like.
</div>
<div class="alert alert-success" role="alert">
  A simple success alert with <a href="#" class="alert-link">an example link</a>. Give it a click if you like.
</div>
<div class="alert alert-danger" role="alert">
  A simple danger alert with <a href="#" class="alert-link">an example link</a>. Give it a click if you like.
</div>
<div class="alert alert-warning" role="alert">
  A simple warning alert with <a href="#" class="alert-link">an example link</a>. Give it a click if you like.
</div>
<div class="alert alert-info" role="alert">
  A simple info alert with <a href="#" class="alert-link">an example link</a>. Give it a click if you like.
</div>
<div class="alert alert-light" role="alert">
  A simple light alert with <a href="#" class="alert-link">an example link</a>. Give it a click if you like.
</div>
<div class="alert alert-dark" role="alert">
  A simple dark alert with <a href="#" class="alert-link">an example link</a>. Give it a click if you like.
</div>

<div class="alert alert-success" role="alert">
  <h4 class="alert-heading">Well done!</h4>
  <p>Aww yeah, you successfully read this important alert message. This example text is going to run a bit longer so that you can see how spacing within an alert works with this kind of content.</p>
  <hr>
  <p class="mb-0">Whenever you need to, be sure to use margin utilities to keep things nice and tidy.</p>
</div>

<svg xmlns="http://www.w3.org/2000/svg" style="display: none;">
  <symbol id="check-circle-fill" fill="currentColor" viewBox="0 0 16 16">
    <path d="M16 8A8 8 0 1 1 0 8a8 8 0 0 1 16 0zm-3.97-3.03a.75.75 0 0 0-1.08.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-.01-1.05z"/>
  </symbol>
  <symbol id="info-fill" fill="currentColor" viewBox="0 0 16 16">
    <path d="M8 16A8 8 0 1 0 8 0a8 8 0 0 0 0 16zm.93-9.412-1 4.705c-.07.34.029.533.304.533.194 0 .487-.07.686-.246l-.088.416c-.287.346-.92.598-1.465.598-.703 0-1.002-.422-.808-1.319l.738-3.468c.064-.293.006-.399-.287-.47l-.451-.081.082-.381 2.29-.287zM8 5.5a1 1 0 1 1 0-2 1 1 0 0 1 0 2z"/>
  </symbol>
  <symbol id="exclamation-triangle-fill" fill="currentColor" viewBox="0 0 16 16">
    <path d="M8.982 1.566a1.13 1.13 0 0 0-1.96 0L.165 13.233c-.457.778.091 1.767.98 1.767h13.713c.889 0 1.438-.99.98-1.767L8.982 1.566zM8 5c.535 0 .954.462.9.995l-.35 3.507a.552.552 0 0 1-1.1 0L7.1 5.995A.905.905 0 0 1 8 5zm.002 6a1 1 0 1 1 0 2 1 1 0 0 1 0-2z"/>
  </symbol>
</svg>

<div class="alert alert-primary d-flex align-items-center" role="alert">
  <svg class="bi flex-shrink-0 me-2" width="24" height="24" role="img" aria-label="Info:"><use xlink:href="#info-fill"/></svg>
  <div>
    An example alert with an icon
  </div>
</div>
<div class="alert alert-success d-flex align-items-center" role="alert">
  <svg class="bi flex-shrink-0 me-2" width="24" height="24" role="img" aria-label="Success:"><use xlink:href="#check-circle-fill"/></svg>
  <div>
    An example success alert with an icon
  </div>
</div>
<div class="alert alert-warning d-flex align-items-center" role="alert">
  <svg class="bi flex-shrink-0 me-2" width="24" height="24" role="img" aria-label="Warning:"><use xlink:href="#exclamation-triangle-fill"/></svg>
  <div>
    An example warning alert with an icon
  </div>
</div>
<div class="alert alert-danger d-flex align-items-center" role="alert">
  <svg class="bi flex-shrink-0 me-2" width="24" height="24" role="img" aria-label="Danger:"><use xlink:href="#exclamation-triangle-fill"/></svg>
  <div>
    An example danger alert with an icon
  </div>
</div>

<div class="alert alert-warning alert-dismissible fade show" role="alert">
  <strong>Holy guacamole!</strong> You should check in on some of those fields below.
  <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
</div>

<h1>Progress</h1>
<div>
<div class="progress">
  <div class="progress-bar" role="progressbar" style="width: 25%;" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100">25%</div>
</div>
<div class="progress margin-top" style="height: 1px;">
  <div class="progress-bar" role="progressbar" style="width: 25%;" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"></div>
</div>

<div class="progress margin-top">
  <div class="progress-bar bg-success" role="progressbar" style="width: 25%" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"></div>
</div>
<div class="progress margin-top">
  <div class="progress-bar bg-info" role="progressbar" style="width: 50%" aria-valuenow="50" aria-valuemin="0" aria-valuemax="100"></div>
</div>
<div class="progress margin-top">
  <div class="progress-bar bg-warning" role="progressbar" style="width: 75%" aria-valuenow="75" aria-valuemin="0" aria-valuemax="100"></div>
</div>
<div class="progress margin-top">
  <div class="progress-bar bg-danger" role="progressbar" style="width: 100%" aria-valuenow="100" aria-valuemin="0" aria-valuemax="100"></div>
</div>
<div class="progress margin-top">
  <div class="progress-bar" role="progressbar" style="width: 15%" aria-valuenow="15" aria-valuemin="0" aria-valuemax="100"></div>
  <div class="progress-bar bg-success" role="progressbar" style="width: 30%" aria-valuenow="30" aria-valuemin="0" aria-valuemax="100"></div>
  <div class="progress-bar bg-info" role="progressbar" style="width: 20%" aria-valuenow="20" aria-valuemin="0" aria-valuemax="100"></div>
</div>
<div class="progress margin-top">
  <div class="progress-bar progress-bar-striped" role="progressbar" style="width: 10%" aria-valuenow="10" aria-valuemin="0" aria-valuemax="100"></div>
</div>
<div class="progress margin-top">
  <div class="progress-bar progress-bar-striped bg-success" role="progressbar" style="width: 25%" aria-valuenow="25" aria-valuemin="0" aria-valuemax="100"></div>
</div>
<div class="progress margin-top">
  <div class="progress-bar progress-bar-striped bg-info" role="progressbar" style="width: 50%" aria-valuenow="50" aria-valuemin="0" aria-valuemax="100"></div>
</div>
<div class="progress margin-top">
  <div class="progress-bar progress-bar-striped bg-warning" role="progressbar" style="width: 75%" aria-valuenow="75" aria-valuemin="0" aria-valuemax="100"></div>
</div>
<div class="progress margin-top">
  <div class="progress-bar progress-bar-striped bg-danger" role="progressbar" style="width: 100%" aria-valuenow="100" aria-valuemin="0" aria-valuemax="100"></div>
</div>
</div>

<div class="margin-bottom">
<h1>List-group</h1>
<ul class="list-group">
  <li class="list-group-item disabled" aria-disabled="true">A disabled item</li>
  <li class="list-group-item">A second item</li>
  <li class="list-group-item">A third item</li>
  <li class="list-group-item">A fourth item</li>
  <li class="list-group-item">And a fifth one</li>
</ul>
<div class="list-group">
  <button type="button" class="list-group-item list-group-item-action active" aria-current="true">
    The current button
  </button>
  <button type="button" class="list-group-item list-group-item-action">A second item</button>
  <button type="button" class="list-group-item list-group-item-action">A third button item</button>
  <button type="button" class="list-group-item list-group-item-action">A fourth button item</button>
  <button type="button" class="list-group-item list-group-item-action" disabled>A disabled button item</button>
</div>

<ul class="list-group">
  <li class="list-group-item">A simple default list group item</li>

  <li class="list-group-item list-group-item-primary">A simple primary list group item</li>
  <li class="list-group-item list-group-item-secondary">A simple secondary list group item</li>
  <li class="list-group-item list-group-item-success">A simple success list group item</li>
  <li class="list-group-item list-group-item-danger">A simple danger list group item</li>
  <li class="list-group-item list-group-item-warning">A simple warning list group item</li>
  <li class="list-group-item list-group-item-info">A simple info list group item</li>
  <li class="list-group-item list-group-item-light">A simple light list group item</li>
  <li class="list-group-item list-group-item-dark">A simple dark list group item</li>
</ul>
</div>

<h1>Figure</h1>
<figure class="figure">
  <svg class="bd-placeholder-img figure-img img-fluid rounded" width="400" height="300" xmlns="http://www.w3.org/2000/svg" role="img" aria-label="Placeholder: 400x300" preserveAspectRatio="xMidYMid slice" focusable="false"><title>Placeholder</title><rect width="100%" height="100%" fill="#868e96"></rect><text x="50%" y="50%" fill="#dee2e6" dy=".3em">400x300</text></svg>
  <figcaption class="figure-caption">A caption for the above image.</figcaption>
</figure>

<div>
<h1>Breadcrumbs</h1>
<nav style="--bs-breadcrumb-divider: '>';" aria-label="breadcrumb">
  <ol class="breadcrumb">
    <li class="breadcrumb-item"><a href="#">Home</a></li>
    <li class="breadcrumb-item active" aria-current="page">Library</li>
  </ol>
</nav>
</div>

<div>
<h1>Spinners</h1>
<div class="spinner-border text-primary" role="status">
  <span class="visually-hidden">Loading...</span>
</div>
<div class="spinner-border text-secondary" role="status">
  <span class="visually-hidden">Loading...</span>
</div>
<div class="spinner-border text-success" role="status">
  <span class="visually-hidden">Loading...</span>
</div>
<div class="spinner-border text-danger" role="status">
  <span class="visually-hidden">Loading...</span>
</div>
<div class="spinner-border text-warning" role="status">
  <span class="visually-hidden">Loading...</span>
</div>
<div class="spinner-border text-info" role="status">
  <span class="visually-hidden">Loading...</span>
</div>
<div class="spinner-border text-light" role="status">
  <span class="visually-hidden">Loading...</span>
</div>
<div class="spinner-border text-dark" role="status">
  <span class="visually-hidden">Loading...</span>
</div>
<br/>
<button class="btn btn-primary" type="button" disabled>
  <span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
  <span class="visually-hidden">Loading...</span>
</button>
<button class="btn btn-primary" type="button" disabled>
  <span class="spinner-border spinner-border-sm" role="status" aria-hidden="true"></span>
  Loading...
</button>
</div>

<div>
<h1>Code</h1>
For example, <code>&lt;section&gt;</code> should be wrapped as inline.
To switch directories, type <kbd>cd</kbd> followed by the name of the directory.<br>
To edit settings, press <kbd><kbd>ctrl</kbd> + <kbd>,</kbd></kbd>
<pre><code>&lt;p&gt;Sample text here...&lt;/p&gt;
&lt;p&gt;And another line of sample text here...&lt;/p&gt;
</code></pre>
<var>y</var> = <var>m</var><var>x</var> + <var>b</var>
</div>
`;

    return (
        <div className="bs5 panel content-margin">
            <h1>Bootstrap 5 playground</h1>

            <div>
                <Example />
                <Carousel2 />
                <ModalUsage />
                <TableExample />
                <CardExample />
                <AlertExample />

                <Button type="button" color="primary">
                    TEST
                </Button>
                <FormExample />
                <AccordionExample />

                <div dangerouslySetInnerHTML={{ __html: myHTML }} />
            </div>
        </div>
    );
}
