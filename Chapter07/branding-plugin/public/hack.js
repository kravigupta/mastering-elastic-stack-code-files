import $ from 'jquery';
import chrome from 'ui/chrome';

console.log("Loading the hack for branding plugin.");

$(document).ready(function(){
    document.title = document.title + " - PACKT";
    console.log("Modified the title");
});
    chrome.setBrand({'title' : "My custom title"});
