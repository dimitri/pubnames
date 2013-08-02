;;;; pubnames.asd

(asdf:defsystem #:pubnames
  :serial t
  :description "Most Popular Pub Names"
  :author "Dimitri Fontaine <dim@tapoueh.org>"
  :license "WTFPL"
  :depends-on (#:postmodern		; PostgreSQL protocol implementation
	       #:cl-postgres		; low level bits for COPY streaming
	       #:cxml			; parsing XML
	       )
  :components ((:file "package")
	       (:file "pubnames" :depends-on ("package"))))

