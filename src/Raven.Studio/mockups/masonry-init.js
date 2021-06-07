

var pckry = new Packery( '.masonry-grid', {
	itemSelector: '.grid-item',
	itemSelector: '.cluster-dashboard-item',
 	percentPosition: true,
 	columnWidth: '.grid-sizer',
 	gutter: '.gutter-sizer',
 	transitionDuration: '0',
});

var draggie = new Draggabilly( '.cluster-dashboard-item', {
	// options...
  });


   document.querySelectorAll('.cluster-dashboard-item').forEach(item => {
		var draggie = new Draggabilly( item );		
		pckry.bindDraggabillyEvents( draggie );
	});




// var msnry = new Masonry( '.masonry-grid', {
// 	itemSelector: '.cluster-dashboard-item',
// 	percentPosition: true,
// 	columnWidth: '.grid-sizer',
// 	gutter: '.gutter-sizer',
// 	transitionDuration: '0',
// });

/////////////////////////////////////////

// var elem = document.querySelector('.masonry-grid');
// var msnry = new Masonry( elem, {
//   // options
//   itemSelector: '.cluster-dashboard-item',
//   percentPosition: true,
//   columnWidth: '.grid-sizer',
//   gutter: 18
// });

// element argument can be a selector string
//   for an individual element

 
//  var $grid = $('.masonry-grid').masonry({        
// 	itemSelector: '.cluster-dashboard-item',
// 	percentPosition: true,
// 	columnWidth: '.grid-sizer',
// 	gutter: 18
// });