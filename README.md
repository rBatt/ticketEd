# [ticketEd](http://ticketed.info/): Learn how to avoid parking tickets

<p align="center">
<img src="./figures/parkingTicket.jpeg?raw=TRUE", width="500">
</p>

ticketEd is a proof-of-concept tool for forecasting parking tickets.

Each year NYC issues over 12 million parking tickets, totaling to over $0.5 billion in revenue.  That's a lot of parking tickets.

And if you've ever tried to park in NYC, or just walked around looking at parking availability, you'd know why. Lots of cars, only a few spots, and lots of parking enforcement officers.

But here's the thing: demand for parking, total number of legal spots (occuped + not), and enforcement intensity vary over time. Therefore, the number tickets varies over time. If you know when ticketing rates are going to be high, you also know when attempting to park is more likely to lead to you getting a ticket. 

That's where ticketEd comes into play. ticketEd forecasts parking tickets for each precinct in NYC. By fitting time series models to NYC Open Data on parking violations, it makes predictions about future ticketing rates, and helps you assess the risk of getting a ticket.

---

