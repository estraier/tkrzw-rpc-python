# Makefile for Tkrzw-RPC for Python

PACKAGE = tkrzw-rpc-python
VERSION = 0.1.3
PACKAGEDIR = $(PACKAGE)-$(VERSION)
PACKAGETGZ = $(PACKAGE)-$(VERSION).tar.gz

PYTHON = python3
RUNENV = LD_LIBRARY_PATH=.:/lib:/usr/lib:/usr/local/lib:$(HOME)/lib

all :
	$(PYTHON) setup.py build
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Ready to install.\n'
	@printf '#================================================================\n'

clean :
	rm -rf casket casket* *~ *.tmp *.tkh *.tkt *.tks *.flat *.log *.so *.pyc build \
	  hoge moge tako ika uni tkrzw_rpc/*~ tkrzw_rpc/__pycache__

install :
	$(PYTHON) setup.py install
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Thanks for using Tkrzw-RPC for Python.\n'
	@printf '#================================================================\n'

uninstall :
	$(PYTHON) setup.py install --record files.tmp
	xargs rm -f < files.tmp

dist :
	$(MAKE) distclean
	rm -Rf "../$(PACKAGEDIR)" "../$(PACKAGETGZ)"
	cd .. && cp -R tkrzw-rpc-python $(PACKAGEDIR) && \
	  tar --exclude=".*" -cvf - $(PACKAGEDIR) | gzip -c > $(PACKAGETGZ)
	rm -Rf "../$(PACKAGEDIR)"
	sync ; sync

distclean : clean apidocclean

check :
	$(RUNENV) $(PYTHON) test.py
	$(RUNENV) $(PYTHON) perf.py --iter 10000 --threads 3
	$(RUNENV) $(PYTHON) perf.py --iter 10000 --threads 3 --random
	$(RUNENV) $(PYTHON) wicked.py --iter 5000 --threads 3
	@printf '\n'
	@printf '#================================================================\n'
	@printf '# Checking completed.\n'
	@printf '#================================================================\n'

apidoc :
	$(MAKE) apidocclean
	mkdir -p tmp-doc/tkrzw_rpc
	cp tkrzw_rpc/*.py tmp-doc/tkrzw_rpc
	cd tmp-doc ; sphinx-apidoc -F -H Tkrzw-RPC -A "Mikio Hirabayashi" -o out .
	cat tmp-doc/out/conf.py |\
	  sed -e 's/^# import /import /' -e 's/^# sys.path/sys.path/' \
	    -e 's/alabaster/haiku/' \
      -e '/sphinx\.ext\.viewcode/d' \
      -e '/^extensions = /a "sphinx.ext.autosummary",' > tmp-doc/out/conf.py.tmp
	echo >> tmp-doc/out/conf.py.tmp
	echo "autodoc_member_order = 'bysource'" >> tmp-doc/out/conf.py.tmp
	echo "html_title = 'Python client library of Tkrzw-RPC'" >> tmp-doc/out/conf.py.tmp
	echo "autodoc_default_options = {'members': True, 'special-members': True, 'exclude-members': '__dict__,__module__,__weakref__'}"  >> tmp-doc/out/conf.py.tmp
	mv -f tmp-doc/out/conf.py.tmp tmp-doc/out/conf.py
	cp -f index.rst tmp-doc/out/index.rst
	cd tmp-doc/out ; $(MAKE) html
	mv tmp-doc/out/_build/html api-doc

apidocclean :
	rm -rf api-doc tmp-doc

protocode : tkrzw_rpc.proto
	$(PYTHON) -m grpc_tools.protoc -I. --python_out=tkrzw_rpc --grpc_python_out=tkrzw_rpc \
	  tkrzw_rpc.proto

.PHONY: all clean install uninstall dist distclean check apidoc apidocclean protocode

# END OF FILE
