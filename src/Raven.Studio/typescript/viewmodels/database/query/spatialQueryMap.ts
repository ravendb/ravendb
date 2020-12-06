import viewModelBase = require("viewmodels/viewModelBase");
import spatialMapLayerModel = require("models/database/query/spatialMapLayerModel");
import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");
import {Layer, MarkerClusterGroup} from "leaflet";

class spatialQueryMap extends viewModelBase {
    
    geoData = ko.observableArray<spatialMapLayerModel>([]);

    constructor(geoData: Array<spatialMapLayerModel>) {
        super();
        this.geoData(geoData);
    }

    compositionComplete() {
        super.compositionComplete();
        this.createMap();
    }
    
    createMap() {

        ////////////////////////////
        // Define base tile layers
        ////////////////////////////

        const osmLink = `<a href="http://openstreetmap.org">OpenStreetMap</a>`;
        const osmUrl = 'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
        const osmAttrib = `&copy; ${osmLink} Contributors`;
        const osmMap = L.tileLayer(osmUrl, {attribution: osmAttrib});

        const otmLink = `<a href="http://opentopomap.org/">OpenTopoMap</a>`;
        const otmUrl = `http://{s}.tile.opentopomap.org/{z}/{x}/{y}.png`;
        const otmAttrib = `&copy; ${otmLink} Contributors`;
        const otmMap = L.tileLayer(otmUrl, {attribution: otmAttrib});

        const baseLayers = {
            "Streets Map": osmMap,
            "Topography Map": otmMap
        };

        //////////////////////////////////////////////////////////////
        // Define group layers (layer per spatial point from query)
        //////////////////////////////////////////////////////////////

        let dataLayers = {};
        let markersGroups: MarkerClusterGroup[] = [];

        this.geoData().forEach((layerModel) => {
            
            let markers: Layer[] = [];
            let markersGroup = L.markerClusterGroup();

            const generatePopupContent = (doc: document) => {
                const docDto = doc.toDto(true);
                const metaDto = docDto["@metadata"];
                documentMetadata.filterMetadata(metaDto);

                let text = JSON.stringify(docDto, null, 4);
                text = Prism.highlight(text, (Prism.languages as any)["javascript"]);

                const textHtml = `<div>
                                    <h4>Document: ${doc.getId()}</h4>
                                    <hr>
                                    <pre>${text}</pre>
                                 </div>`;
                return textHtml;
            }

            layerModel.geoPoints().forEach((point) => {
                const pointMarker = L.marker([point.latitude, point.longitude], { title: point.popupContent.getId() })
                                     .bindPopup(generatePopupContent(point.popupContent), { 'className' : 'custom-popup', 'maxWidth': 600, 'maxHeight': 400 } );
                markers.push(pointMarker);
            });

            markersGroup.addLayers(markers);
            const controlLayerText = `<span>Point fields:</span>
                                      <div class='margin-left'>
                                          <span>${layerModel.latitudeProperty}</span><br>
                                          <span>${layerModel.longitudeProperty}</span>
                                      </div>`;
            (dataLayers as any)[controlLayerText] = markersGroup;
            markersGroups.push(markersGroup);
        })

        //////////////////////
        // Define the map 
        //////////////////////

        let map = L.map('mapid', {preferCanvas: true});      // init map
        osmMap.addTo(map);                                   // init base layer to use
        L.control.layers(baseLayers, dataLayers).addTo(map); // init legend control
        markersGroups.forEach(group => map.addLayer(group)); // init markers groups
        
        let bounds = markersGroups[0].getBounds();
        for (let i = 1; i <= markersGroups.length-1; i++) {
            bounds.extend(markersGroups[i].getBounds());
        }

        map.fitBounds(bounds, { padding: [50, 50] });
    }
}

export = spatialQueryMap;
