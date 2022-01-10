
window.dash_clientside = Object.assign({}, window.dash_clientside, {
    clientside: {
        CND: function(x){

            var a1, a2, a3, a4, a5, k;

            a1 = 0.31938153, a2 =-0.356563782, a3 = 1.781477937, a4= -1.821255978, a5= 1.330274429;

            if(x<0.0)
                return 1 - CND(-x);
                        else
                k = 1.0 / (1.0 + 0.2316419 * x);
                        return 1.0 - Math.exp(-x * x / 2.0) / Math.sqrt(2 * Math.PI) * k
                            * (a1 + k * (-0.356563782 + k * (1.781477937 + k * (-1.821255978 + k * 1.330274429)))) ;

        },

        blackScholes: function (volPrice, CoP, X, v, Xp, vp, S, r, Sp, rp, month) {
            
            var delta, price, gamma, T;

            //cumlative normal dist
            function CND(x) {

                var a1, a2, a3, a4, a5, k;

                a1 = 0.31938153, a2 = -0.356563782, a3 = 1.781477937, a4 = -1.821255978, a5 = 1.330274429;

                if (x < 0.0)
                    return 1 - CND(-x);
                else
                    k = 1.0 / (1.0 + 0.2316419 * x);
                return 1.0 - Math.exp(-x * x / 2.0) / Math.sqrt(2 * Math.PI) * k
                    * (a1 + k * (-0.356563782 + k * (1.781477937 + k * (-1.821255978 + k * 1.330274429))));

            };

            function PDF(x) {
                return (1/Math.sqrt(2*Math.PI))*Math.exp(-(x*x)/2)
            }

            //get date diff in days
            function datediff(first, second) {
                // Take the difference between the dates and divide by milliseconds per day.
                // Round to nearest whole number to deal with DST.
                return Math.round((second - first) / (1000 * 60 * 60 * 24));
            }
            //black scholes pricing function (LME)
            function bs(CoP,S,X,r,v,T){

                var df = Math.exp(-r * (T+2/52))
                var vT = v * Math.sqrt(T);
                var d1 = (Math.log(S / X) + (r + v * v / 2.0) * T) / vT;
                var d2 = d1 - vT;


                if (CoP == 'c') {
                    delta = CND(d1)
                    cnd2 = CND(d2)                     
                    return df*(S * delta - X * cnd2)}
                else {
                    delta = CND(-d1)
                    cnd2 = CND(-d2)     
                    price = df*(X * cnd2 - S * delta)
                    return df*(X * CND(-d2) - S * delta)}
            }

            function option_implied_volatility(CoP,S,X,r,T,o) { 
                // CoP = Boolean (to calc call, call=True, put: call=false)
                // S = stock prics, X = strike price, r = no-risk interest rate
                // t = time to maturity
                // o = option price
                 
                // define some temp vars, to minimize function calls
                  sqt = Math.sqrt(T);
                  MAX_ITER = 100;
                  ACC = 0.1;

                  df = Math.exp(-r * (T+2/52));
                  sigma = (o/S)/(0.398*sqt);
                  for (i=0;i<MAX_ITER;i++) {
                    price = bs(CoP,S,X,r,sigma,T);
                    diff = o-price;
                    if (Math.abs(diff) < ACC) {return sigma};

                    vT = sigma * sqt
                     
                    d1 = (Math.log(S / X) + (r + sigma * sigma / 2.0) * T) / vT;
                    d2 = d1 - vT;                    
                    vega = (X * df * PDF(d2) * Math.sqrt(T));                    
                    sigma = sigma+diff/vega;
                    
                  }
                  return "Error";
                
                } 

            //todays date
            let today = new Date()
            today.setHours(0)
            today.setMinutes(0)
           
            //let expiry = new Date( month.charAt(0).toUpperCase() + month.slice(1,3).toLowerCase() +' 01 20'+ month.slice(3, 5))
            var parts = month.split('-');
            var expiry = new Date(parts[0], parts[1]-1, parts[2]);

            var T = (datediff(today, expiry)) / 365

            //replace with value 
            var S = (S) ? S : Sp;
            var X = (X) ? X : Xp;
            var r = (r) ? r : rp;
            var v = (v) ? v : vp;
            
            r=r/100
            //if price then back out vol                            
            if (volPrice=='price'){
             v=option_implied_volatility(CoP,S,X,r,T,v)               
                }           
            else {v= v/100}         
    
            var df = Math.exp(-r * (T+2/52));      
            var vT = v * Math.sqrt(T);  
            var d1 = (Math.log(S / X) + (r + v**2  / 2.0) * T) / vT;
            var d2 = d1 - vT;
        
            delta = CND(d1)
            cnd2 = CND(d2) 

            gamma = df * delta / (S * vT)
            vega = (X * df * PDF(d2) * Math.sqrt(T)) / 100

            a = -(S * PDF(d1) * v) / (2 * Math.sqrt(T))
            b = r * S * df * delta
            c = r * X * df * CND(-d1)

            theta = (a + b - c) / 365
            rho = X * T * df * cnd2 * 0.01
          

            if (CoP == 'c') {
                delta = CND(d1)
                cnd2 = CND(d2) 

                price = df*(S * delta - X * cnd2)
                price = bs('c',S,X,r,v,T)
                return [Math.round(price * 100) / 100,
                Math.round(delta * 100) / 100,
                Math.round(gamma * 100000) / 100000,
                Math.round(vega * 100) / 100,
                Math.round(theta * 100) / 100,
                Math.round(v * 100*100) / 100,
                ];
              
            }
            else {
                delta = CND(-d1)
                cnd2 = CND(-d2) 

                price = df*(X * cnd2 - S * delta)

                return [Math.round(price * 100) / 100,
                Math.round(-delta * 100) / 100,
                Math.round(gamma * 100000) / 100000,
                Math.round(vega * 100) / 100,
                Math.round(theta * 100) / 100,
                Math.round(v * 100*100) / 100,
                ];

            }

        },

        forward_calc: function(b, bp, s, sp){
            //replace with value 
            var b = (b) ? b : bp;
            var s = (s) ? s : sp;

            return b

        }

        // stratColor: function (strat) {
        //    var green =  {'background':'#bbf07a'}
        //    var red = {'background':'#f54747'}
        //    var blank ={'background':'#fafafa'}
                      
        //    if (strat == 'outright')  {
        //        return [green, blank, blank, blank]
        //    }
        //    else if (strat == 'spread') {
        //     return [green, red, blank, blank]
        //    }
        //    else if (strat == 'straddle') {
        //     return [green, green, blank, blank]
        //    }
        //    else if (strat == 'fly') {
        //     return [green, red, green, blank]
        //    }       
        //    else if (strat == 'condor') {
        //     return [green, red, red, green]
        //    }                
        //    else if (strat == 'ladder') {
        //     return [green, red, red, blank]
        //    }            
        //    else if (strat == 'ratio') {
        //     return [green, red, blank, blank]
        //    }             
        //    else {
        //     return [blank, blank, blank, blank]
        //    }

        // }

    }
});
