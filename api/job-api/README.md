# Job Server

The Job Server is a microservice that abstracts out the retrieval and updates of jobs.  It can potentially pull from many locations, but is the endpoint for the Engine9 scheduler to retrieve new jobs, and push updates and errors back to those jobs.  It can be run standalone, or included as part of the object api server.

