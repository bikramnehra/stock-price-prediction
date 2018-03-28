/* global angular, document, window */
'use strict';

angular.module('starter.controllers', [])

.controller('AppCtrl', function($scope, $ionicModal, $ionicPopover, $timeout) {
    // Form data for the login modal
    $scope.loginData = {};
    $scope.isExpanded = false;
    $scope.hasHeaderFabLeft = false;
    $scope.hasHeaderFabRight = false;

    var navIcons = document.getElementsByClassName('ion-navicon');
    for (var i = 0; i < navIcons.length; i++) {
        navIcons.addEventListener('click', function() {
            this.classList.toggle('active');
        });
    }

    ////////////////////////////////////////
    // Layout Methods
    ////////////////////////////////////////

    $scope.hideNavBar = function() {
        document.getElementsByTagName('ion-nav-bar')[0].style.display = 'none';
    };

    $scope.showNavBar = function() {
        document.getElementsByTagName('ion-nav-bar')[0].style.display = 'block';
    };

    $scope.noHeader = function() {
        var content = document.getElementsByTagName('ion-content');
        for (var i = 0; i < content.length; i++) {
            if (content[i].classList.contains('has-header')) {
                content[i].classList.toggle('has-header');
            }
        }
    };

    $scope.setExpanded = function(bool) {
        $scope.isExpanded = bool;
    };

    $scope.setHeaderFab = function(location) {
        var hasHeaderFabLeft = false;
        var hasHeaderFabRight = false;

        switch (location) {
            case 'left':
                hasHeaderFabLeft = true;
                break;
            case 'right':
                hasHeaderFabRight = true;
                break;
        }

        $scope.hasHeaderFabLeft = hasHeaderFabLeft;
        $scope.hasHeaderFabRight = hasHeaderFabRight;
    };

    $scope.hasHeader = function() {
        var content = document.getElementsByTagName('ion-content');
        for (var i = 0; i < content.length; i++) {
            if (!content[i].classList.contains('has-header')) {
                content[i].classList.toggle('has-header');
            }
        }

    };

    $scope.hideHeader = function() {
        $scope.hideNavBar();
        $scope.noHeader();
    };

    $scope.showHeader = function() {
        $scope.showNavBar();
        $scope.hasHeader();
    };

    $scope.clearFabs = function() {
        var fabs = document.getElementsByClassName('button-fab');
        if (fabs.length && fabs.length > 1) {
            fabs[0].remove();
        }
    };
})

.controller('LoginCtrl', function($scope, $timeout, $stateParams, ionicMaterialInk) {
    $scope.$parent.clearFabs();
    $timeout(function() {
        $scope.$parent.hideHeader();
    }, 0);
    ionicMaterialInk.displayEffect();
})

.controller('PortfolioCtrl', function($scope, $stateParams, $timeout, ionicMaterialMotion, ionicMaterialInk, $http) {
    // Set Header
    $scope.$parent.showHeader();
    $scope.$parent.clearFabs();
    $scope.isExpanded = false;
    $scope.$parent.setExpanded(false);
    $scope.$parent.setHeaderFab(false);


    // Set Motion
    $timeout(function() {
        ionicMaterialMotion.slideUp({
            selector: '.slide-up'
        });
    }, 300);

    $timeout(function() {
        ionicMaterialMotion.fadeSlideInRight({
            startVelocity: 3000
        });
    }, 700);

    // Set Ink
    ionicMaterialInk.displayEffect();


    $scope.options = {
        chart: {
            type: 'lineChart',
            height: 450,
            margin: {
                top: 20,
                right: 20,
                bottom: 60,
                left: 65
            },

            x: function(d) {
                return d[0];
            },
            y: function(d) {
                return d[1];
            },

            color: [
                "#D22908",
                '#FFA500',
                '#28B662'
                ],
            duration: 300,
            useInteractiveGuideline: false,
            interactive: true,
            tooltips: true,
            tooltip: {
                contentGenerator: function(d) {
                    console.log(d);
                    return '<div class="nvtooltip xy-tooltip nv-pointer-events-none" \
                                id="nvtooltip-37493" style="top: '+d.pos.top+';left:'+d.pos.left+';opacity: 1; \
                                position: relative;">\
                                <table><thead><tr><td colspan="3"><strong class="x-value">'+d3.time.format('%d/%m/%Y')(new Date(d.point[0]))+ '\
                                </strong></td></tr></thead><tbody><tr><td class="legend-color-guide">\
                                <div style="background-color: '+d.series[0].color+';"></div>\
                                </td><td class="key"><strong>Return: </strong></td><td>'+d.point[1]+'</td></tr>\
                                </tbody></table><strong>'+d.point[2]+'</strong></div>';
                }
            },

            clipVoronoi: false,

            xAxis: {
                axisLabel: 'Dates',
                tickFormat: function(d) {
                    return d3.time.format('%d/%m/%Y')(new Date(d))
                },
                showMaxMin: false,
                staggerLabels: true
            },

            yAxis: {
                axisLabel: 'Returns',
                axisLabelDistance: 0
            }
        }
    };

    var highRisk = []
    var mediumRisk = []
    var lowRisk = []

    $http.get("http://bikramnehra.pythonanywhere.com/portfolio")
        .success(function(response) {
            $scope.data = response;

            angular.forEach($scope.data, function(value, key) {

                if(value.risk == "HighRisk"){
                    highRisk.push([new Date(value.date).valueOf(), Math.round(value.value * 100) / 100, value.companies]);
                }
                else if(value.risk == "MediumRisk"){
                    mediumRisk.push([new Date(value.date).valueOf(), Math.round(value.value * 100) / 100, value.companies]);
                }
                else if(value.risk == "LowRisk"){
                    lowRisk.push([new Date(value.date).valueOf(), Math.round(value.value * 100) / 100, value.companies]);
                }
                
            });

            $scope.data = [

                {
                    "key": "High Risk",
                    "values": highRisk,
                },

                {
                    "key": "Medium Risk",
                    "values": mediumRisk,

                },

                {
                    "key": "Low Risk",
                    "values": lowRisk,

                }


            ];
        });

})

.controller('TrendsCtrl', function($scope, $stateParams, $timeout, ionicMaterialMotion, ionicMaterialInk, $http, $log) {

    //Auto Suggest Companies
    $scope.simulateQuery = false;
    $scope.isDisabled = false;
    // list of tickrs to be displayed
    $scope.tickrs = loadTickrs();
    $scope.querySearch = querySearch;
    $scope.selectedItemChange = selectedItemChange;
    $scope.searchTextChange = searchTextChange;

    function querySearch(query) {
        var results = query ? $scope.tickrs.filter(createFilterFor(query)) : $scope.tickrs,
            deferred;
        if ($scope.simulateQuery) {
            deferred = $q.defer();
            $timeout(function() {
                    deferred.resolve(results);
                },
                Math.random() * 1000, false);
            return deferred.promise;
        } else {
            return results;
        }
    }

    function searchTextChange(text) {
        $log.info('Text changed to ' + text);
    }

    function selectedItemChange(item) {
        $log.info('Item changed to ' + JSON.stringify(item));
        if (item) {
            renderGraph(item);
        }
    }

    function loadTickrs() {
        var tickrVals = []
        $http.get('tickr.json').success(function(data) {
            for (var key in data) {
                tickrVals.push({
                    value: key,
                    display: data[key]
                })
            }
        });
        return tickrVals;
    }

    //filter function for search query
    function createFilterFor(query) {
        var lowercaseQuery = angular.lowercase(query);
        return function filterFn(state) {
            return (state.display.toLowerCase().indexOf(lowercaseQuery) === 0);
        };
    }

    var tickrList = ['AAPL', 'GOOG'];

    // Charts
    function renderGraph(tickr) {

        $scope.options = {
            chart: {
                type: 'lineChart',
                height: 450,
                margin: {
                    top: 20,
                    right: 20,
                    bottom: 60,
                    left: 65
                },

                x: function(d) {
                    return d[0];
                },
                y: function(d) {
                    return d[1];
                },

                color: d3.scale.category10().range(),
                duration: 300,
                useInteractiveGuideline: true,
                clipVoronoi: false,

                caption: {
                    enable: true,
                    text: 'Title for Line Chart'
                },

                xAxis: {
                    axisLabel: 'Dates',
                    tickFormat: function(d) {
                        return d3.time.format('%d/%m/%Y')(new Date(d))
                    },
                    showMaxMin: false,
                    staggerLabels: true
                },

                yAxis: {
                    axisLabel: 'Stock Prices',

                    axisLabelDistance: 0
                }
            }
        };

        var pNews = []
        var pNewsPrices = []
        var actualValues = []

        $http.get("http://bikramnehra.pythonanywhere.com/stocks?tickr=" + tickr.value)
            .success(function(response) {
                $scope.data = response;

                angular.forEach($scope.data, function(value, key) {
                    actualValues.push([new Date(value.Date).valueOf(), Math.round(value.AClose * 100) / 100]);
                    pNews.push([new Date(value.Date).valueOf(), Math.round(value.pNews * 100) / 100]);
                    pNewsPrices.push([new Date(value.Date).valueOf(), Math.round(value.pNewsPrices * 100) / 100]);
                });

                $scope.data = [

                    {
                        "key": "Actual",
                        "values": actualValues

                    },

                    {
                        "key": "Predicted with News",
                        "values": pNews

                    },

                    {
                        "key": "Predicted with News and Prices",
                        "values": pNewsPrices

                    }


                ];
            });

    }

});