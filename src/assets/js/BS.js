window.dash_clientside = Object.assign({}, window.dash_clientside, {
  clientside: {
    blackScholes: function (volPrice, CoP, X, v, Xp, vp, S, r, Sp, rp, month) {
      // var cnd1, price, gamma, T;

      // Cumulative normal dist, based on Eq. 26.2.19 from Abramowitz and Stegun
      // "Handbook of Mathematical Functions" 1972
      function CND(x) {
        const a1 = 0.049867347;
        const a2 = 0.0211410061;
        const a3 = 0.0032776263;
        const a4 = 0.0000380036;
        const a5 = 0.0000488906;
        const a6 = 0.000005383;

        if (x >= 0.0) {
          return (
            1.0 -
            0.5 *
            Math.pow(
              1.0 +
              (((((a6 * x + a5) * x + a4) * x + a3) * x + a2) * x + a1) * x,
              -16
            )
          );
        } else {
          let y = -x;
          return (
            0.5 *
            Math.pow(
              1.0 +
              (((((a6 * y + a5) * y + a4) * y + a3) * y + a2) * y + a1) * y,
              -16
            )
          );
        }
      }

      function PDF(x) {
        return Math.pow(2 * Math.PI, -0.5) * Math.exp(-(x * x) * 0.5);
      }

      //get date diff in days
      function datediff(first, second) {
        // Take the difference between the dates and divide by milliseconds per day.
        // Round to nearest whole number to deal with DST.
        return Math.round((second - first) / (1000 * 60 * 60 * 24));
      }
      //black scholes pricing function (LME)
      function bs(CoP, S, X, rc, v, T) {
        let df = Math.exp(-rc * (T + 14 / 365));
        let vT = v * Math.sqrt(T);
        let d1 = (Math.log(S / X) + ((v * v) * 0.5) * T) / vT;
        let d2 = d1 - vT;

        if (CoP == "c") {
          return df * (S * CND(d1) - X * CND(d2));
        } else {
          return df * (X * CND(-d2) - S * CND(-d1));
        }
      }

      function bs_price(cop, strike, underlying, cont_rate, volatility, time_to_expiry) {
        let discount_factor = Math.exp(-cont_rate * (time_to_expiry + 14 / 365));
        let vol_sqrt_time = volatility * Math.sqrt(time_to_expiry);
        let d1_val = (Math.log(underlying / strike) + 0.5 * vol_sqrt_time * vol_sqrt_time) / vol_sqrt_time;
        let d2_val = d1_val - vol_sqrt_time;

        if (cop == "c") {
          return discount_factor * (underlying * CND(d1_val) - strike * CND(d2_val))
        }
        else {
          return discount_factor * (strike * CND(-d2_val) - underlying * CND(-d1_val))
        }
      }

      function option_implied_volatility(CoP, S, X, r, T, o) {
        // CoP = Boolean (to calc call, call=True, put: call=false)
        // S = stock prics, X = strike price, r = no-risk interest rate
        // t = time to maturity
        // o = option price

        // define some temp vars, to minimize function calls
        let sqt = Math.sqrt(T);
        const MAX_ITER = 200;
        const ACC = 0.005;

        let df = Math.exp(-r * (T + 14 / 365)); //discount factor LME specific
        let sigma = o / S / (0.398 * sqt);
        let diff;
        let price;

        for (i = 0; i < MAX_ITER; i++) {
          price = bs(CoP, S, X, r, sigma, T);
          diff = o - price;
          if (Math.abs(diff) < ACC) {
            return sigma;
          }

          vT = sigma * sqt;

          // d2 = (Math.log(S / X) + (-(sigma * sigma) / 2.0) * T) / vT;
          // d2 = d1 - vT;
          sigma =
            sigma +
            diff /
            (X *
              df *
              PDF((Math.log(S / X) + sigma * sigma * 0.5 * T) / vT) *
              Math.sqrt(T));
        }
        return "Error";
      }

      //todays date
      let today = new Date();
      today.setHours(0);
      today.setMinutes(0);

      //let expiry = new Date( month.charAt(0).toUpperCase() + month.slice(1,3).toLowerCase() +' 01 20'+ month.slice(3, 5))
      var parts = month.split("-");
      var expiry = new Date(parts[0], parts[1] - 1, parts[2]);

      var T = (datediff(today, expiry)) / 365;

      //replace with value
      var S = S ? S : Sp;
      var X = X ? X : Xp;
      var r = r ? r : rp;
      var v = v ? v : vp;

      // let rc = r / 100.0;
      let rc = Math.log(1 + r / 100.0);
      console.log(r, rc)
      //if price then back out vol
      if (volPrice == "price") {
        v = option_implied_volatility(CoP, S, X, rc, T, v);
      } else {
        v = v / 100.0;
      }

      var df = Math.exp(-rc * T);
      // let premium_df =  Math.exp(-rc * (T+ 14/365));
      var vT = v * Math.sqrt(T);
      var d1 = (Math.log(S / X) + 0.5 * v * v * T) / vT;
      var d2 = d1 - vT;

      let cnd1;
      let cnd2;

      let theta_a = (-df * S * PDF(d1) * v) / (2 * Math.sqrt(T));
      let theta_b;
      let theta_c;
      let theta;

      let gamma = (df * PDF(d1)) / (S * vT);
      let vega = (X * df * PDF(d2) * Math.sqrt(T)) / 100;

      if (CoP == "c") {
        cnd1 = CND(d1);
        cnd2 = CND(d2);
        theta_b = rc * X * df * cnd2;
        theta_c = rc * S * df * cnd1;

        theta = (theta_a - theta_b + theta_c) / 365;

        let price = bs_price(CoP, X, S, rc, v, T);

        return [
          Math.round(price * 100) / 100,
          Math.round(df * cnd1 * 10000) / 10000,
          Math.round(gamma * 100000) / 100000,
          Math.round(vega * 1000) / 1000,
          Math.round(theta * 1000) / 1000,
          Math.round(v * 100 * 1000) / 1000,
        ];
      } else {
        cnd1 = CND(-d1);
        cnd2 = CND(-d2);
        theta_b = rc * X * df * cnd2;
        theta_c = rc * S * df * cnd1;

        theta = (theta_a + theta_b - theta_c) / 365;

        let price = bs_price(CoP, X, S, rc, v, T);

        return [
          Math.round(price * 100) / 100,
          Math.round(-df * cnd1 * 10000) / 10000,
          Math.round(gamma * 100000) / 100000,
          Math.round(vega * 1000) / 1000,
          Math.round(theta * 1000) / 1000,
          Math.round(v * 100 * 1000) / 1000,
        ];
      }
    },

    blackScholesEU: function (volPrice, nowOpen, CoP, X, v, Xp, vp, S, r, Sp, rp, month) {
      // var cnd1, price, gamma, T;

      // Cumulative normal dist, based on Eq. 26.2.19 from Abramowitz and Stegun
      // "Handbook of Mathematical Functions" 1972
      function CND(x) {
        const a1 = 0.049867347;
        const a2 = 0.0211410061;
        const a3 = 0.0032776263;
        const a4 = 0.0000380036;
        const a5 = 0.0000488906;
        const a6 = 0.000005383;

        if (x >= 0.0) {
          return (
            1.0 -
            0.5 *
            Math.pow(
              1.0 +
              (((((a6 * x + a5) * x + a4) * x + a3) * x + a2) * x + a1) * x,
              -16
            )
          );
        } else {
          let y = -x;
          return (
            0.5 *
            Math.pow(
              1.0 +
              (((((a6 * y + a5) * y + a4) * y + a3) * y + a2) * y + a1) * y,
              -16
            )
          );
        }
      }

      function PDF(x) {
        return Math.pow(2 * Math.PI, -0.5) * Math.exp(-(x * x) * 0.5);
      }

      //get date diff in days
      function datediff(first, second) {
        // Take the difference between the dates and divide by milliseconds per day.
        // Round to nearest whole number to deal with DST.
        return Math.round((second - first) / (1000 * 60 * 60 * 24));
      }

      //get date diff in days
      function now_datediff(first, second) {
        // Take the difference between the dates and divide by milliseconds per day.
        // Round to nearest whole number to deal with DST.
        return ((second - first) / (1000 * 60 * 60 * 24));
      }

      //black scholes pricing function (LME)
      function bs(CoP, S, X, rc, v, T) {
        let df = Math.exp(-rc * (T));    //DELETED 14/365
        let vT = v * Math.sqrt(T);
        let d1 = (Math.log(S / X) + ((v * v) * 0.5) * T) / vT;
        let d2 = d1 - vT;

        if (CoP == "c") {
          return df * (S * CND(d1) - X * CND(d2));
        } else {
          return df * (X * CND(-d2) - S * CND(-d1));
        }
      }

      function bs_price(cop, strike, underlying, cont_rate, volatility, time_to_expiry) {
        let discount_factor = Math.exp(-cont_rate * (time_to_expiry));  //DELETED 14/365
        let vol_sqrt_time = volatility * Math.sqrt(time_to_expiry);
        let d1_val = (Math.log(underlying / strike) + 0.5 * vol_sqrt_time * vol_sqrt_time) / vol_sqrt_time;
        let d2_val = d1_val - vol_sqrt_time;

        if (cop == "c") {
          return discount_factor * (underlying * CND(d1_val) - strike * CND(d2_val))
        }
        else {
          return discount_factor * (strike * CND(-d2_val) - underlying * CND(-d1_val))
        }
      }

      function option_implied_volatility(CoP, S, X, r, T, o) {
        // CoP = Boolean (to calc call, call=True, put: call=false)
        // S = stock prics, X = strike price, r = no-risk interest rate
        // t = time to maturity
        // o = option price

        // define some temp vars, to minimize function calls
        let sqt = Math.sqrt(T);
        const MAX_ITER = 200;
        const ACC = 0.005;

        let df = Math.exp(-r * (T));  // deleted  + 14 / 365
        let sigma = o / S / (0.398 * sqt);
        let diff;
        let price;

        for (i = 0; i < MAX_ITER; i++) {
          price = bs(CoP, S, X, r, sigma, T);
          diff = o - price;
          if (Math.abs(diff) < ACC) {
            return sigma;
          }

          vT = sigma * sqt;

          // d2 = (Math.log(S / X) + (-(sigma * sigma) / 2.0) * T) / vT;
          // d2 = d1 - vT;
          sigma =
            sigma +
            diff /
            (X *
              df *
              PDF((Math.log(S / X) + sigma * sigma * 0.5 * T) / vT) *
              Math.sqrt(T));
        }
        return "Error";
      }

      // get time to expiry, open or now 
      if (nowOpen == "open") {
        //todays date
        let today = new Date();
        today.setHours(0);
        today.setMinutes(0);

        //let expiry = new Date( month.charAt(0).toUpperCase() + month.slice(1,3).toLowerCase() +' 01 20'+ month.slice(3, 5))
        var parts = month.split("-");
        var expiry = new Date(parts[0], parts[1] - 1, parts[2]);

        var T = (datediff(today, expiry)) / 365;
      } else {
        //todays date, time not reset
        let today = new Date();
        // today.setHours(0);
        // today.setMinutes(0);

        //let expiry = new Date( month.charAt(0).toUpperCase() + month.slice(1,3).toLowerCase() +' 01 20'+ month.slice(3, 5))
        var parts = month.split("-");
        var expiry = new Date(parts[0], parts[1] - 1, parts[2]);

        var T = (now_datediff(today, expiry)) / 365;
      }

      //replace with value
      var S = S ? S : Sp;
      var X = X ? X : Xp;
      var r = r ? r : rp;
      var v = v ? v : vp;

      // let rc = r / 100.0;
      let rc = Math.log(1 + r / 100.0);
      console.log(r, rc)
      //if price then back out vol
      if (volPrice == "price") {
        v = option_implied_volatility(CoP, S, X, rc, T, v);
      } else {
        v = v / 100.0;
      }

      var df = Math.exp(-rc * T);
      // let premium_df =  Math.exp(-rc * (T+ 14/365));
      var vT = v * Math.sqrt(T);
      var d1 = (Math.log(S / X) + 0.5 * v * v * T) / vT;
      var d2 = d1 - vT;

      let cnd1;
      let cnd2;

      let theta_a = (-df * S * PDF(d1) * v) / (2 * Math.sqrt(T));
      let theta_b;
      let theta_c;
      let theta;

      let gamma = (df * PDF(d1)) / (S * vT);
      let vega = (X * df * PDF(d2) * Math.sqrt(T)) / 100;

      if (CoP == "c") {
        cnd1 = CND(d1);
        cnd2 = CND(d2);
        theta_b = rc * X * df * cnd2;
        theta_c = rc * S * df * cnd1;

        theta = (theta_a - theta_b + theta_c) / 365;

        let price = bs_price(CoP, X, S, rc, v, T);

        return [
          Math.round(price * 100) / 100,
          Math.round(df * cnd1 * 10000) / 10000,
          Math.round(gamma * 100000) / 100000,
          Math.round(vega * 1000) / 1000,
          Math.round(theta * 1000) / 1000,
          Math.round(v * 100 * 1000) / 1000,
        ];
      } else {
        cnd1 = CND(-d1);
        cnd2 = CND(-d2);
        theta_b = rc * X * df * cnd2;
        theta_c = rc * S * df * cnd1;

        theta = (theta_a + theta_b - theta_c) / 365;

        let price = bs_price(CoP, X, S, rc, v, T);

        return [
          Math.round(price * 100) / 100,
          Math.round(-df * cnd1 * 10000) / 10000,
          Math.round(gamma * 100000) / 100000,
          Math.round(vega * 1000) / 1000,
          Math.round(theta * 1000) / 1000,
          Math.round(v * 100 * 1000) / 1000,
        ];
      }
    },

    forward_calc: function (b, bp, s, sp) {
      //replace with value
      var b = b ? b : bp;
      var s = s ? s : sp;

      return b;
    },
  },
});
